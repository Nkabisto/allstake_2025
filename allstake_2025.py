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
    # Parse times first 
    df = df.with_columns([
            # Parse time columns to Datetime
            pl.col(["arrival_time","finish_time","departure_time"]).str.strptime(pl.Datetime, format="%I:%M %p", strict=False),

            # Cast numeric columns to Float64
            pl.col(["amount_paid","duration","hours_worked","bonuses","deductions"]).cast(pl.Float64, strict=False),
        ])
    # Calculate duration from times 
    df = df.with_columns([
        # Calculate time-based duration
        pl.when(pl.col("finish_time") < pl.col("arrival_time"))
        .then((pl.col("finish_time") + pl.duration(days=1) - pl.col("arrival_time")).dt.total_hours())
        .otherwise((pl.col("finish_time") - pl.col("arrival_time")).dt.total_hours())
        .alias("time_based_duration")
    ])

    # Final duration: prefer existing duration, fall back to time-based calculation
    return df.with_columns([
        pl.coalesce(
            pl.col("duration").fill_nan(pl.col("hours_worked")),
            pl.col("time_based_duration")
        ).alias("duration")
    ]).drop("time_based_duration")

def printTables(con:connection):
    tables = ['staging_booking_tb', 'staging_financials_tb', 'staging_jobs_tb','staging_stocktaker_tb']
    for tb in tables:
        df = ingest_table(tb,con)

        print(df.sample(min(100,df.height)))
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
  #          printTables(con)
            booking_df = ingest_table("staging_booking_tb",con)
            booking_df = bookingTransformations(booking_df)
            print(booking_df["duration","hours_worked"].sample(30))

    except Exception as e:
        logger.error(f"Error detected: {e}")


