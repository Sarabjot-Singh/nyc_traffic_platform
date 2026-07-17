import sys
import shutil
import tempfile
import requests
import yaml
from datetime import datetime
from pathlib import Path
from pyspark.sql.functions import *

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.common.spark import SparkManager
from src.common.database import Database
from repository.metadata_query import QueryStore
from src.bronze.yellow_trip.schema import schema

CONFIG_PATH = 'config.yaml'
with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

RAW_CONFIG_PATH = './src/bronze/raw_config.yml'
with open(RAW_CONFIG_PATH, "r", encoding="utf-8") as file:
    raw_config = yaml.safe_load(file)

logger = get_logger()


# Ingest NYC taxi parquet files from the public source and stage them in the bronze layer.
# This function walks through a range of year-month partitions and processes each file
# independently so the pipeline can recover gracefully from partial failures.
def ingest_data(
        spark, 
        start_year, 
        start_month, 
        end_year=datetime.now().year, 
        end_month=datetime.now().month
    ) -> None:
    """Ingest NYC taxi parquet files for a range of year-month partitions.

    The function downloads each parquet file from the public NYC Taxi source,
    validates it with the bronze schema, and writes it to the configured bronze
    storage location while recording its status in the metadata table.

    Args:
        spark: Active Spark session used to read and write parquet data.
        start_year: Starting year of the ingestion range.
        start_month: Starting month of the ingestion range.
        end_year: Ending year of the ingestion range.
        end_month: Ending month of the ingestion range.
    """

    # Read the bronze storage location and dataset identity from configuration.
    # These values determine where the staged files are written and which dataset is ingested.
    raw = config["layers"]["bronze"]
    bucket_name = config["storage"]["bucket_name"]
    source_name = raw_config['datasets']['yellow_trip']['source_name']
    name = raw_config['datasets']['yellow_trip']['name']
    database_obj = Database()

    # Build the sequence of year-month partitions to process.
    # The loop expands the requested start/end range into a list of monthly buckets.
    year_month_list = []

    while start_year <= end_year:
        # Stop once the current month reaches the requested end month.
        if start_year == end_year and start_month == end_month:
            break

        # Normalize the month to a two-digit string so the partition names are consistent.
        month_str = "0" + str(start_month) if len(str(start_month)) == 1 else str(start_month)
        year_month_list.append(str(start_year) + "-" + month_str)

        # Advance to the next month, rolling over to the next year when needed.
        if start_month == 12:
            start_month = 1
            start_year += 1
            continue
        start_month += 1

    for year_month in year_month_list:
        logger.info(f"{favicon['info']} Fetching data for %s", year_month)
        file_name = f"{source_name}_{year_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        file_year = year_month.split("-")[0]
        file_month = year_month.split("-")[1]
        s3_key = rf"s3a://{bucket_name}/{raw}/{name}/year={file_year}/month={file_month}/"
        
        rs = database_obj.execute(QueryStore.is_file_Uploaded_to_bronze(file_name=file_name))
        is_file_present = rs.fetchone()[0]
        
        # Skip files that already completed successfully in the metadata log.
        # This prevents duplicate work and keeps repeated runs idempotent.
        if not is_file_present:
            try:
                upload_query = QueryStore().ingestion_log(file_name, url, s3_key, 'UPLOADING', None)
                database_obj.execute(query=upload_query, params={})

                # Download the parquet file to a temporary location before loading it with Spark.
                with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:

                    with requests.get(url, stream=True) as response:
                        response.raise_for_status()
                        shutil.copyfileobj(response.raw, tmp)

                    # Make sure all bytes are written
                    tmp.flush()

                    # Read the temporary parquet file with Spark so the data can be validated
                    # and cast to the expected schema before writing to the bronze layer.
                    df = spark.read.parquet(tmp.name)

                    # Apply the bronze schema definitions only where the source columns exist.
                    for column, dtype in schema.items():
                        if column in df.columns:
                            df = df.withColumn(column, col(column).cast(dtype))
            
                    # Write the curated parquet data to the bronze storage path.
                    df.write \
                        .mode("overwrite") \
                        .parquet(s3_key)

                # Record a successful bronze load in the metadata table.
                # This gives downstream jobs a reliable audit trail for processed files.
                success_query = QueryStore().ingestion_log(file_name, url, s3_key, 'SUCCESS', None)
                database_obj.execute(query=success_query, params={})
                logger.info(f"{favicon['right']} Successfully uploaded %s to S3", s3_key)

            except Exception as e:
                logger.info(f"{favicon['error']} Error occured during uploading file to S3: ", e)
                # Mark the file as failed so it can be retried later by the next run.
                success_query = QueryStore().ingestion_log(file_name, url, s3_key, 'FAILED', str(e))
                database_obj.execute(query=success_query, params={})

        else:
            logger.info(f"{favicon['info']} File %s altready present in the bucket", file_name)


if __name__ == "__main__":
    # Entry point for running the bronze ingestion job from the command line.
    logger.info(f"{favicon['info']} Start fetching data from NYC CDN")
    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()

    start_year = raw_config["datasets"]['yellow_trip']["start_year"]
    start_month = raw_config["datasets"]['yellow_trip']["start_month"]
    end_year = raw_config["datasets"]['yellow_trip']["end_year"]
    end_month = raw_config["datasets"]['yellow_trip']["end_month"]

    # Please add end dated as per requirements, if not passed latest date will be considered for end year and end month
    ingest_data(spark, start_year, start_month)