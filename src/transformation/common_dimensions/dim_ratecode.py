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
from src.common.spark import SparkManager
from src.transformation.common_dimensions.dimensionalModel import DimensionalModel

logger = get_logger()

spark = SparkManager("dim_rate_code").get_spark_session()

with open('./seeds/seeds.yml', 'r') as file:
    config = yaml.safe_load(file)



class DimRateCode(DimensionalModel):

    def __init__(self):
        self.schema = StructType([
                StructField("rate_code_id", IntegerType(), False),
                StructField("description", StringType(), False)
            ])
    
    def initial_load(self):
        """
            Create the dim_rate_code dimension table from seed data.
        """
        try:
            logger.info(f"{favicon['info']} Starting dim_rate_code transformation")

            rate_code_df = spark.read.format('csv') \
                                .schema(self.schema) \
                                .option('header', True) \
                                .option('mergeSchema', True) \
                                .load(config['seeds']['dim_ratecode']['path'])
            
            rate_code_df = rate_code_df.withColumn("rate_code_sk", row_number().over(
                Window.orderBy("rate_code_id")
            )).select("rate_code_sk", "rate_code_id", "description")
            
            logger.info(f"{favicon['right']} dim_rate_code transformation completed successfully")
            return rate_code_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error during dim_rate_code transformation: %s", str(e))
            return None
        
        
    def incremental_load(self):
        """
        Incremental load for dim_rate_code checks for dim_rate_code in S3 and if not found, performs initial load. 
        If found, it checks for new vendors in seed files and appends them to the existing dim_rate_code table.
        """
        try:
            original_df = spark.read.parquet("s3a://nyc-traffic-spark-2026/dimensions/dim_ratecode.parquet")

            logger.info(f"{favicon['info']} dim_ratecode table found in S3, checking for new vendors...")

            incoming_vendors_df = spark.read.format('csv') \
                                    .option('header', True) \
                                    .option('mergeSchema', True) \
                                    .schema(self.schema) \
                                    .load(config['seeds']['dim_ratecode']['path'])
            
            max_sk = original_df.agg({"rate_code_sk": "max"}).collect()[0][0]
            
            new_vendors_df = incoming_vendors_df.join(original_df, on='rate_code_id', how='left_anti')
            new_vendors_df = new_vendors_df.withColumn("rate_code_sk", row_number().over(
                Window.orderBy("rate_code_id")
            ) + max_sk).select("rate_code_sk", "rate_code_id", "description")

            vendors_df = original_df.union(new_vendors_df)

            return vendors_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error while reading dim_rate_code from S3: %s", str(e))
            return None


if __name__ == '__main__':
    dim_ratecode = DimRateCode()
    df = dim_ratecode.initial_load()
    df.show()