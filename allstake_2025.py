import os
import datetime as dt
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


def ingest_table(table_name:str, con:connection, schema_overrides:dict=None)->pl.DataFrame:
    logger.info(f"Ingesting table: {table_name}")
    try:
        df = pl.read_database(query=f'SELECT * FROM {table_name}',connection=con, schema_overrides= schema_overrides)
    except Exception as e:
        raise RuntimeError(f"Database read failed from {e}")

    return df

def bookingTransformations(df:pl.DataFrame)->pl.DataFrame:
    logger.info("Transforming the booking table")
    # Parse times first 
    df = df.with_columns([
            # Parse time columns to Datetime
            pl.col(["arrival_time","finish_time","departure_time"]).str.strptime(pl.Datetime, format="%I:%M %p", strict=False),

            # Cast numeric columns to Float64
            pl.col(["amount_paid","duration","hours_worked","bonuses","deductions"]).cast(pl.Float64, strict=False),
        ])
    # Calculate duration from times 
    logger.info("Calculating the duration column using the finish_time and arrival_time columns")
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
            pl.col("duration")
            .fill_nan(pl.col("hours_worked")),
            pl.col("time_based_duration")
        ).alias("duration")
    ]).drop("time_based_duration")

def transformFinancialsTbl(df:pl.DataFrame)->pl.DataFrame:
    logger.info("Transforming the Financials table numeric columns from type string to Float64")
    # Cast to numeric
    numeric_cols = [
            "counter_cost_hr",
            "scanner_cost_hr",
            "auditor_controller_cost_hr",
            "assistant_co_ordinator_co_hr",
            "co_ordinator_cost_hr",
            "updates_amount",
            "paysheet_amount"
        ]
    exprs = [
        (
            pl.when(pl.col(col).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(col))
                .cast(pl.Float64, strict=False) # now safely cast
                .alias(col)
        )
        for col in numeric_cols
    ]

    return df.with_columns(exprs)

def transformJobsTbl(df:pl.DataFrame)->pl.DataFrame:
    logger.info("Transforming the Jobs table columns")

    return df.with_columns(
        pl.col("date_of_job").cast(pl.Date, strict=False)
    )

def printTables(con:connection):
    tables = ['staging_booking_tb', 'staging_financials_tb', 'staging_jobs_tb','staging_stocktaker_tb']
    for tb in tables:
        df = ingest_table(tb,con)

        print(df.sample(min(100,df.height)))
        print(df.columns)

def getAmountPaid(df:pl.DataFrame)->pl.DataFrame:
    logger.info("Calculating the amount_paid column")
    # Build conditional expression
    condition = pl.when(pl.col("job_position") == "COUNTER").then(pl.col("counter_cost_hr"))
    conditions = {
        "SCANNER":  "scanner_cost_hr",
        "AUDITOR":  "auditor_controller_cost_hr",
        "CONTROLLER": "auditor_controller_cost_hr",
        "ASS COORD": "assistant_co_ordinator_co_hr",
        "COORD": "co_ordinator_cost_hr"
    }

    # --- Defensive check: ensure required columns exists ---
    required_cols = {"job_position", "duration", "bonuses", "deductions","counter_cost_hr"} | set(conditions.values())
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns in DataFrame: {missing}")

    for job_position, cost_col in conditions.items():
        condition = condition.when(pl.col("job_position") == job_position).then(pl.col(cost_col))

    hourly_rate_expr = (
        condition
        .otherwise(0.0)
        .cast(pl.Float64)
        .fill_null(0.0)
        .fill_nan(0.0)
    )


    logger.info("Calculating the final amount_paid using the hourly_rate_expr, duration, bonuses and deductions columns")
    # Calculate final amount
    return df.with_columns(
        (
            hourly_rate_expr
            * pl.col("duration").cast(pl.Float64, strict=False).fill_null(0.0).fill_nan(0.0)
            + pl.col("bonuses").cast(pl.Float64, strict=False).fill_null(0.0).fill_nan(0.0)
            - pl.col("deductions").cast(pl.Float64, strict=False).fill_null(0.0).fill_nan(0.0)
        ).alias("amount_paid")
    )

def getPaysheetTotal(paysheet:str)->pl.DataFrame:
    logger.info(f"Generating a dataframe for file named: {paysheet}")

    schema = {
        "INVOICE NO.":pl.Utf8,
        "AMOUNT PAID": pl.Utf8
    }

    df = pl.read_csv(paysheet,skip_rows=3, columns=["INVOICE NO.","AMOUNT PAID"], schema_overrides=schema)
    df_filter_asterix = df.filter((pl.col("AMOUNT PAID") != "*") &  ~pl.col("INVOICE NO.").is_null())
    
    logger.info("Cleaning 'AMOUNT PAID' by removing currency symbols")
    df = df_filter_asterix.select(
        pl.col("INVOICE NO.").alias("invoice_number"),
        pl.col("AMOUNT PAID")
        .str.replace_all(r"[^0-9\.\-]+", "") # Regex: Remove any character that is not a digit, a period (.), or a negative sign(-)
        .str.strip_chars()          # Remove any leading/trailing spaces leftover
        .cast(pl.Float64,strict=False) # Now safely cast the clean string to a float
        .alias("amount_paid")
    )

    q = (
        df.lazy()
        .group_by("invoice_number")
        .agg(
            pl.col("amount_paid").sum().alias("stocktake_totals")
        )
    )
    return q.collect()


def extractAllPaysheetsDF(folderPath:str)->pl.DataFrame:
    dfs = []    # list to collect dataframes
    for filename in os.listdir(folderPath):
        file_path = os.path.join(folderPath, filename)
        dfs.append(getPaysheetTotal(file_path))
    
    # Concatenate into one main dataframe
    logger.info("Concatenating all dataframes into one main dataframe")
    return pl.concat(dfs)

if __name__ == "__main__":
    db_name = os.getenv("DB_NAME")
    db_host= os.getenv("DB_HOST")
    db_pwd= os.getenv("DB_PWD")
    db_port= os.getenv("DB_PORT")
    db_user= os.getenv("DB_USER")

    conn_string = f"dbname={db_name} user={db_user} password={db_pwd} host={db_host} port={db_port}"
    try:
        with psycopg.connect(conn_string) as con:
            financials_schema_overrides = {
                "job_number":pl.Categorical,
                "status": pl.Enum(["Planning","Cancelled","Payment Received","Invoiced"])
            }
            financials_df = transformFinancialsTbl(ingest_table("staging_financials_tb",con,financials_schema_overrides))
            job_cost_df = financials_df["job_number","status","counter_cost_hr","scanner_cost_hr","auditor_controller_cost_hr","assistant_co_ordinator_co_hr","co_ordinator_cost_hr"]

            booking_schema_overrides = {
                "student_number":pl.Categorical,
                "job_number": pl.Categorical,
                "booked": pl.Enum(["To Be Booked","Replaced","Booked","DP","Replacing"]),
                "group_name": pl.Categorical,
                "rating": pl.Categorical,
                "job_position": pl.Enum(["","COUNTER","SCANNER","AUDITOR","CONTROLLER","ASS COORD","COORD"]),
                "responsible_for_qc": pl.Categorical
            }
           
            booking_df = ingest_table("staging_booking_tb",con,booking_schema_overrides)
            booking_df = bookingTransformations(booking_df)

            df = getAmountPaid(booking_df.join(job_cost_df,on="job_number",how="inner"))

            logger.info("Filtering out the booking rows with empty job_number column values")
            filter_missing_job_numbers_df = df.filter(pl.col("job_number") != '') # job_number column values cannot be empty
            filter_missing_job_numbers_df.write_parquet("./bookings.parquet")

            q = (
                filter_missing_job_numbers_df.lazy()
                .group_by("job_number")
                .agg(
                    pl.col("amount_paid").sum().alias("updates_totals")
                )
                .sort("job_number", descending=True)
            )
            booking_totals_df = q.collect()

            # Join the  'updates_amount' and 'paysheet_amount' columns for comparison with the updates_totals
            financials_amount_cols  = financials_df.select(
                pl.col("job_number"),
                pl.col("updates_amount"),
                pl.col("paysheet_amount"),
                pl.col("invoice_number")
            )

            compare_stocktake_totals_df = booking_totals_df.join(financials_amount_cols,on="job_number", how="inner").sort("job_number", descending=True)
            
            jobs_df = transformJobsTbl(ingest_table("staging_jobs_tb", con, {"job_number":pl.Categorical}))
            jobs_df = jobs_df.select(
                pl.col("job_number"),
                pl.col("name"),
                pl.col("date_of_job")
            )

            logger.info("Adding stocktake names, and dates from jobs table")
            compare_stocktake_totals_df = compare_stocktake_totals_df.join(jobs_df, on="job_number", how="inner").sort("job_number",descending=True)

            logger.info("Filtering date_of_job to between 2025-03-01 and today")

            compare_stk_totals_filtered_df = compare_stocktake_totals_df.filter(
               pl.col("date_of_job").is_between(dt.date(2025,3,1), dt.date(2025,10,31))
            ).sort("date_of_job", descending=False)

            logger.info("Comparing stocktake totals between the aggregated amounts vs one from the 'paysheet'")

            das_jobs_totals_csv = compare_stk_totals_filtered_df.write_parquet("./das_jobs_totals.parquet")
            print(compare_stocktake_totals_df["job_number","name","date_of_job","updates_totals","invoice_number"])

            folder_path = "./CSVs"
            main_ps_df =  extractAllPaysheetsDF(folder_path)
            print(main_ps_df)

            final_das_jobs_df = compare_stocktake_totals_df.join(main_ps_df, on="invoice_number", how="inner")
            print(final_das_jobs_df)
            logger.info("Converting Categorical columns to Utf8 for a safe CSV export.")
            final_das_jobs_df.write_parquet("./stocktake_summary.parquet")

    except Exception as e:
        logger.error(f"Error detected: {e}")


