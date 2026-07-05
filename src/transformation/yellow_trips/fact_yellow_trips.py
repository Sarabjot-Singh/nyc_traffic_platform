import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon

load_dotenv()
logger = get_logger()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC Taxi Transformation") 

    # Required packages
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.4.1",
            "io.delta:delta-spark_2.13:4.0.0"
        ])
    )
    .config("spark.jars.ivy", "/tmp/.ivy2")
    # MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)


def transform_nyc_taxi_data():
    """
    Transform NYC taxi data from raw to processed format.
    """
    


if __name__ == "__main__":
    logger.info(f"{favicon['info']} Starting transformation pipeline")
    transform_nyc_taxi_data()