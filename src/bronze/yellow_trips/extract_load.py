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
from src.bronze.yellow_trips.schema import schema

CONFIG_PATH = 'config.yaml'
with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

logger = get_logger()


def ingest_data(
        spark, 
        start_year, 
        start_month, 
        end_year=datetime.now().year, 
        end_month=datetime.now().month
    ) -> None:
    """
    Get historical data from the NYC Taxi CDN.
    """

    # S3 Location Configuration Variables
    raw = config["layers"]["bronze"]
    bucket_name = config["storage"]["bucket_name"]
    source_name = config['datasets']['yellow_trip']['source_name']
    database_obj = Database()

    # Additional variables for tracking the current year and month
    year_month_list = []

    while start_year <= end_year:
        if start_year == end_year and start_month == end_month:
            break
        month_str = "0" + str(start_month) if len(str(start_month)) == 1 else str(start_month)
        year_month_list.append(str(start_year) + "-" + month_str)
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
        s3_key = rf"s3a://{bucket_name}/{raw}/{source_name}/year={file_year}/month={file_month}/"
        
        rs = database_obj.execute(f"""
                SELECT COUNT(1) FROM metadata.ingestion_log WHERE file_name = '{file_name}' AND status = 'SUCCESS'
        """)
        is_file_present = rs.fetchone()[0]
        
        if not is_file_present:
            try:
                upload_query = QueryStore().ingestion_log(file_name, url, s3_key, 'UPLOADING')
                database_obj.execute(query=upload_query, params={})

                with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:

                    with requests.get(url, stream=True) as response:
                        response.raise_for_status()
                        shutil.copyfileobj(response.raw, tmp)

                    # Make sure all bytes are written
                    tmp.flush()

                    # Read using Spark
                    df = spark.read.parquet(tmp.name)

                    for column, dtype in schema.items():
                        if column in df.columns:
                            df = df.withColumn(column, col(column).cast(dtype))

                    df.printSchema()
            
                    df.write \
                        .mode("append") \
                        .parquet(s3_key)

                success_query = QueryStore().ingestion_log(file_name, url, s3_key, 'SUCCESS')
                database_obj.execute(query=success_query, params={})
                logger.info(f"{favicon['right']} Successfully uploaded %s to S3", s3_key)

            except Exception as e:
                logger.info(f"{favicon['error']} Error occured during uploading file to S3: ", e)
                success_query = QueryStore().ingestion_log(file_name, url, s3_key, 'FAILED')
                database_obj.execute(query=success_query, params={})

        else:
            logger.info(f"{favicon['info']} File %s altready present in the bucket", file_name)


if __name__ == "__main__":
    logger.info(f"{favicon['info']} Start fetching data from NYC CDN")
    source_name = config['datasets']['yellow_trip']['source_name']
    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()

    start_year = config["datasets"]['yellow_trip']["start_year"]
    start_month = config["datasets"]['yellow_trip']["start_month"]
    end_year = config["datasets"]['yellow_trip']["end_year"]
    end_month = config["datasets"]['yellow_trip']["end_month"]

    # Please add end dated as per requirements, if not passed latest date will be considered for end year and end month
    ingest_data(spark, start_year, start_month)