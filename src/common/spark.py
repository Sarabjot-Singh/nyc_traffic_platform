import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

class SparkManager:
    def __init__(self, app_name: str = "NYC Taxi Transformation"):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
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

    def get_spark_session(self):
        return self.spark