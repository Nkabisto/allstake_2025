import os
from datetime import datetime
import polars as pl
import polars.selectors as cs
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

def ingest_table(table_name:str, con:connection)->pl.DataFrame:
    try:
        df = pl.read_database(query=f'SELECT * FROM {table_name}',connection=con)
    except Exception as e:
        raise RuntimeError("Database read failed") from e

    return df

def bookingTransformations(df:pl.DataFrame)->pl.DataFrame:
    return df.select(cs.exclude('responsible_for_qc','last_updated_at','raw_data','ingestion_timestamp')
    )

def printTables(con:connection):
    tables = ['staging_booking_tb', 'staging_financials_tb', 'staging_jobs_tb','staging_stocktaker_tb']
    for tb in tables:
        df = ingest_tables(tb,con)

        print(df.sample(min(10,df.height)))
        print(df.columns)

if __name__ == "__main__":
    db_name = os.getenv("DB_NAME")
    db_host= os.getenv("DB_HOST")
    db_pwd= os.getenv("DB_PWD")
    db_port= os.getenv("DB_PORT")
    db_user= os.getenv("DB_USER")

    conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"
    try:
        with psycopg.connect(conn_string) as con:
            booking_df = ingest_table("staging_booking_tb",con)
            booking_df = bookingTransformations(booking_df)
            print(booking_df.sample(20))
            
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure PostgreSQL is running: sudo systemctl start postgresql")


