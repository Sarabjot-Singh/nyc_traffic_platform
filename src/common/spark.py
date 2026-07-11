import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

load_dotenv()

# Shared helper for creating and reusing the Spark session used by the pipeline.
class SparkManager:
    """Create and reuse a Spark session configured for the NYC taxi pipeline.

    The session is initialized with the required Delta and S3-compatible settings so
    downstream bronze and silver jobs can share the same execution context.
    """

    def __init__(self, app_name: str = "NYC Taxi Transformation"):
        """Initialize the Spark session for the provided application name.

        Args:
            app_name: Name to assign to the Spark application.
        """
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
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_URL"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        ).getOrCreate()

    def get_spark_session(self):
        """Return the configured Spark session instance.

        Returns:
            SparkSession: The shared Spark session for the pipeline.
        """
        # Return the existing Spark session so downstream jobs can reuse it.
        return self.spark