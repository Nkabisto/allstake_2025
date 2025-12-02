import os
from datetime import datetime
import polars as pl
from dotenv import load_dotenv
from psycopg2 import connect
import logging

# Log events of interest
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
logger.adHandler(console_hanlder)

load_dotenv()

if __name__ == "__main__":
    db_name = os.getenv("DB_NAME")
    db_host= os.getenv("DB_HOST")
    db_pwd= os.getenv("DB_PWD")
    db_port= os.getenv("DB_PORT")
    db_user= os.getenv("DB_USER")

    conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"

    try:
        with psycopg2.connect(conn_string) as con:

            async with ClientSession() as client:
                with con.cursor() as cur:
                    cur.execute()
                await populate_board_ids_on_das_jobs(client,con)
                logger.info("Inserted board ids on the external board ids column on the DAS Jobs board")
                await populate_db_with_monday_board_two_stage(con, client, "Students")
                logger.info("Activelist pipeline completed successfully.")
                model_names = ["Job","Store","Financials","Feedback"]
                await ingest_das_jobs_board_two_stage(con,client,model_names)
                logger.info("DAS Jobs pipeline completed successfully.")
                await populate_db_with_monday_board_two_stage(con, client, "Booking")
                logger.info("Bookings pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure PostgreSQL is running: sudo systemctl start postgresql")

