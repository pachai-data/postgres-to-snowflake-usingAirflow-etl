import os
from pathlib import Path

import pandas as pd
import psycopg
import snowflake.connector
from dotenv import load_dotenv


# 1. LOAD CONFIGURATION


PROJECT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_DIR / ".env")

LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

PIPELINE_NAME = "postgres_orders_to_snowflake"

PG_CONFIG = {
    "host": os.environ["PG_HOST"],
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.environ["PG_DATABASE"],
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

SF_CONFIG = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
}



# 2. CREATE SNOWFLAKE TABLES

def create_tables(sf_conn):
    with sf_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ORDERS_CLEAN (
                ORDER_ID NUMBER(38, 0) PRIMARY KEY,
                CUSTOMER_NAME VARCHAR(100),
                CITY VARCHAR(50),
                PRODUCT VARCHAR(100),
                QUANTITY NUMBER(38, 0),
                AMOUNT NUMBER(10, 2),
                ORDER_DATE TIMESTAMP_NTZ,
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ETL_CONTROL (
                PIPELINE_NAME VARCHAR(100) PRIMARY KEY,
                LAST_ORDER_ID NUMBER(38, 0) NOT NULL
            )
        """)

        cur.execute("""
            SELECT LAST_ORDER_ID
            FROM ETL_CONTROL
            WHERE PIPELINE_NAME = %s
        """, (PIPELINE_NAME,))

        if cur.fetchone() is None:
            cur.execute("""
                INSERT INTO ETL_CONTROL
                    (PIPELINE_NAME, LAST_ORDER_ID)
                VALUES (%s, %s)
            """, (PIPELINE_NAME, 0))

    sf_conn.commit()


# 3. EXTRACT NEW RECORDS FROM POSTGRESQL

def extract_data(last_order_id):
    query = """
        SELECT
            order_id,
            customer_name,
            city,
            product,
            quantity,
            amount,
            order_date
        FROM orders_source
        WHERE order_id > %s
        ORDER BY order_id
    """

    with psycopg.connect(**PG_CONFIG) as pg_conn:
        df = pd.read_sql_query(
            query,
            pg_conn,
            params=(last_order_id,)
        )

    print(f"Extracted records: {len(df)}")
    return df


# 4. TRANSFORM AND VALIDATE DATA

def transform_data(df):
    if df.empty:
        return df.copy(), df.copy()

    df = df.copy()

    df = df.drop_duplicates(
        subset=["order_id"],
        keep="last"
    )

 
    for col in ["customer_name", "city", "product"]:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .replace("", pd.NA)
        )

    df["customer_name"] = df["customer_name"].str.title()
    df["city"] = df["city"].str.title()
    df["product"] = df["product"].str.title()

 
    df["quantity"] = pd.to_numeric(
        df["quantity"], errors="coerce"
    )
    df["amount"] = pd.to_numeric(
        df["amount"], errors="coerce"
    )

    df["order_date"] = pd.to_datetime(
        df["order_date"], errors="coerce"
    )

    required_columns = [
        "order_id",
        "customer_name",
        "city",
        "product",
        "quantity",
        "amount",
        "order_date",
    ]

    invalid = df[required_columns].isna().any(axis=1)

    invalid |= df["quantity"].le(0)
    invalid |= df["amount"].lt(0)

    invalid |= df["quantity"].mod(1).ne(0)

    rejected_df = df[invalid].copy()
    clean_df = df[~invalid].copy()

    if not rejected_df.empty:
        reject_file = LOG_DIR / "rejected_orders.csv"

        rejected_df.to_csv(
            reject_file,
            mode="a",
            header=not reject_file.exists(),
            index=False
        )

        print(f"Rejected records: {len(rejected_df)}")
        print(f"Rejected data saved to: {reject_file}")

    print(f"Valid records: {len(clean_df)}")

    return clean_df, df



# 5. LOAD DATA AND UPDATE CHECKPOINT

def load_data(sf_conn, clean_df, highest_source_id):
    insert_query = """
        INSERT INTO ORDERS_CLEAN (
            ORDER_ID,
            CUSTOMER_NAME,
            CITY,
            PRODUCT,
            QUANTITY,
            AMOUNT,
            ORDER_DATE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
     
        with sf_conn.cursor() as cur:
            cur.execute("BEGIN")

            if not clean_df.empty:
                records = []

                for row in clean_df.itertuples(index=False):
                    records.append((
                        int(row.order_id),
                        str(row.customer_name),
                        str(row.city),
                        str(row.product),
                        int(row.quantity),
                        float(row.amount),
                        row.order_date.to_pydatetime(),
                    ))

                cur.executemany(insert_query, records)

          
            cur.execute("""
                UPDATE ETL_CONTROL
                SET LAST_ORDER_ID = %s
                WHERE PIPELINE_NAME = %s
            """, (int(highest_source_id), PIPELINE_NAME))

        sf_conn.commit()

        print(f"Loaded records: {len(clean_df)}")
        print(f"Checkpoint advanced to: {highest_source_id}")

    except Exception:
        sf_conn.rollback()
        raise



# 6. MAIN ETL WORKFLOW

def main():
    pg_conn = None
    sf_conn = None

    try:
        print("ETL pipeline started")

        # Connect to Snowflake.
        sf_conn = snowflake.connector.connect(**SF_CONFIG)

        create_tables(sf_conn)

        with sf_conn.cursor() as cur:
            cur.execute("""
                SELECT LAST_ORDER_ID
                FROM ETL_CONTROL
                WHERE PIPELINE_NAME = %s
            """, (PIPELINE_NAME,))

            last_order_id = int(cur.fetchone()[0])

        print(f"Last processed order ID: {last_order_id}")

        # Extract.
        source_df = extract_data(last_order_id)

        if source_df.empty:
            print("No new records. Nothing to process.")
            return

        highest_source_id = int(source_df["order_id"].max())

        # Transform.
        clean_df, _ = transform_data(source_df)

        # Load and checkpoint.
        load_data(sf_conn, clean_df, highest_source_id)

        print("ETL pipeline completed successfully")

    except Exception as exc:
        print(f"ETL pipeline failed: {exc}")
        raise

    finally:
        if pg_conn is not None:
            pg_conn.close()

        if sf_conn is not None:
            sf_conn.close()


if __name__ == "__main__":
    main()
