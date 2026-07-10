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



class DimVendors(Model):

    def __init__(self, spark_session):
        self.schema = StructType([
                StructField("vendor_id", IntegerType(), False),
                StructField("description", StringType(), False)
            ])
        self.spark = spark_session
    
    def initial_load(self):
        """
            Create the dim_vendors dimension table from seed data.
        """
        try:
            logger.info(f"{favicon['info']} Starting dim_vendors transformation")

            vendors_df = self.spark.read.format('csv') \
                                .schema(self.schema) \
                                .option('header', True) \
                                .option('mergeSchema', True) \
                                .load(config['seeds']['dim_vendors']['path'])
            
            vendors_df = vendors_df.withColumn("vendor_sk", row_number().over(
                Window.orderBy("vendor_id")
            )).select("vendor_sk", "vendor_id", "description")
            
            logger.info(f"{favicon['right']} dim_vendors transformation completed successfully")
            return vendors_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error during dim_vendors transformation: %s", str(e))
            return None
        
    def incremental_load(self):
        """
        Incremental load for dim_vendors checks for dim_vendors in S3 and if not found, performs initial load. 
        If found, it checks for new vendors in seed files and appends them to the existing dim_vendors table.
        """
        try:
            original_df = self.spark.read.parquet("s3a://nyc-traffic-spark-2026/dimensions/dim_vendors.parquet")

            logger.info(f"{favicon['info']} dim_vendors table found in S3, checking for new vendors...")

            incoming_vendors_df = self.spark.read.format('csv') \
                                    .option('header', True) \
                                    .option('mergeSchema', True) \
                                    .schema(self.schema) \
                                    .load(config['seeds']['dim_vendors']['path'])
            
            max_sk = original_df.agg({"vendor_sk": "max"}).collect()[0][0]
            
            new_vendors_df = incoming_vendors_df.join(original_df, on='vendor_id', how='left_anti')
            new_vendors_df = new_vendors_df.withColumn("vendor_sk", row_number().over(
                Window.orderBy("vendor_id")
            ) + max_sk).select("vendor_sk", "vendor_id", "description")

            vendors_df = original_df.union(new_vendors_df)

            return vendors_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error while reading dim_vendors from S3: %s", str(e))
            return None