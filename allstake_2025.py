import os
from datetime import datetime
import polars as pl
from dotenv import load_dotenv
import psycopg
from psycopg import connection
import logging

# Log events of interest
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
logger.addHandler(console_handler)

load_dotenv()

def ingest_tables(table_name:str, con:connection)->pl.DataFrame:
    try:
        df = pl.read_database(query=f'SELECT * FROM {table_name}',connection=con)
    except Exception as e:
        raise RuntimeError("Database read failed") from e

    print(df.sample(min(10,df.height)))
    print(df.columns)

    return df

if __name__ == "__main__":
    db_name = os.getenv("DB_NAME")
    db_host= os.getenv("DB_HOST")
    db_pwd= os.getenv("DB_PWD")
    db_port= os.getenv("DB_PORT")
    db_user= os.getenv("DB_USER")

    conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"
    try:
        with psycopg.connect(conn_string) as con:
            tables = ['staging_booking_tb', 'staging_financials_tb', 'staging_jobs_tb','staging_stocktaker_tb']
            for tb in tables:
                df = ingest_tables(tb,con)

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure PostgreSQL is running: sudo systemctl start postgresql")


