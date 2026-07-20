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



class DimLocation():

    def __init__(self, spark_session):
        self.schema = StructType([
                StructField("location_id", IntegerType(), False),
                StructField("borough", StringType(), True),
                StructField("zone", StringType(), True),
                StructField("service_zone", StringType(), True),
            ])
        self.spark = spark_session
    
    def initial_load(self):
        """
            Create the dim_location dimension table from seed data.
        """
        try:
            logger.info(f"{favicon['info']} Starting dim_location transformation")

            rate_code_df = self.spark.read.format('csv') \
                                .schema(self.schema) \
                                .option('header', True) \
                                .option('mergeSchema', True) \
                                .load(config['seeds']['dim_location']['path'])
            
            rate_code_df = rate_code_df.withColumn("location_sk", row_number().over(
                Window.orderBy("location_id")
            )).select("location_sk", "location_id", "borough", "zone", "service_zone")
            
            logger.info(f"{favicon['right']} dim_location transformation completed successfully")
            return rate_code_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error during dim_location transformation: %s", str(e))
            return None
        
        
    def incremental_load(self):
        """Incrementally load the dim_location dimension table.

        Checks if dim_location already exists in S3. If not found, performs initial load.
        If found, compares against new locations in seed files and appends any new locations
        to the existing dim_location table.
        """
        try:
            original_df = self.spark.read.parquet("s3a://nyc-traffic-spark-2026/dimensions/dim_location.parquet")

            logger.info(f"{favicon['info']} dim_location table found in S3, checking for new locations...")

            incoming_locations_df = self.spark.read.format('csv') \
                                    .option('header', True) \
                                    .option('mergeSchema', True) \
                                    .schema(self.schema) \
                                    .load(config['seeds']['dim_location']['path'])
            
            max_sk = original_df.agg({"location_sk": "max"}).collect()[0][0]
            
            new_locations_df = incoming_locations_df.join(original_df, on='location_id', how='left_anti')
            new_locations_df = new_locations_df.withColumn("location_sk", row_number().over(
                Window.orderBy("location_id")
            ) + max_sk).select("location_sk", "location_id", "borough", "zone", "service_zone")

            locations_df = original_df.union(new_locations_df)

            return locations_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error while reading dim_location from S3: %s", str(e))
            return None