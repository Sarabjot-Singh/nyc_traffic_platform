import sys
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.window import Window

from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.silver.base import Model

logger = get_logger()

with open('./seeds/seeds.yml', 'r') as file:
    config = yaml.safe_load(file)



class DimPaymentMethod():

    def __init__(self, spark_session):
        self.schema = StructType([
                StructField("payment_method_id", IntegerType(), False),
                StructField("description", StringType(), False)
            ])
        self.spark = spark_session
    
    def initial_load(self):
        """Create the dim_payment_method dimension table from seed data."""
        try:
            logger.info(f"{favicon['info']} Starting dim_payment_method transformation")

            payment_method_df = self.spark.read.format('csv') \
                                .schema(self.schema) \
                                .option('header', True) \
                                .option('mergeSchema', True) \
                                .load(config['seeds']['dim_payment_method']['path'])
            
            payment_method_df = payment_method_df.withColumn("payment_method_sk", row_number().over(
                Window.orderBy("payment_method_id")
            )).select("payment_method_sk", "payment_method_id", "description")
            
            logger.info(f"{favicon['right']} dim_payment_method transformation completed successfully")
            return payment_method_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error during dim_payment_method transformation: %s", str(e))
            return None
        
        
    def incremental_load(self):
        """Incrementally load the dim_payment_method dimension table.

        Checks if dim_payment_method already exists in S3. If not found, performs initial load.
        If found, compares against new payment methods in seed files and appends any new payment methods
        to the existing dim_payment_method table.
        """
        try:
            original_df = self.spark.read.parquet("s3a://nyc-traffic-spark-2026/dimensions/dim_payment_method.parquet")

            logger.info(f"{favicon['info']} dim_payment_method table found in S3, checking for new payment methods...")

            incoming_payment_methods_df = self.spark.read.format('csv') \
                                    .option('header', True) \
                                    .option('mergeSchema', True) \
                                    .schema(self.schema) \
                                    .load(config['seeds']['dim_payment_method']['path'])
            
            max_sk = original_df.agg({"payment_method_sk": "max"}).collect()[0][0]
            
            new_payment_methods_df = incoming_payment_methods_df.join(original_df, on='payment_method_id', how='left_anti')
            new_payment_methods_df = new_payment_methods_df.withColumn("payment_method_sk", row_number().over(
                Window.orderBy("payment_method_id")
            ) + max_sk).select("payment_method_sk", "payment_method_id", "description")

            payment_methods_df = original_df.union(new_payment_methods_df)

            return payment_methods_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error while reading dim_payment_method from S3: %s", str(e))
            return None


