import sys
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType, BooleanType, LongType
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



class DimDate():

    def __init__(self, spark_session):
        self.schema = StructType([
                StructField("date", DateType(), False),
                StructField("date_key", LongType(), False),
                StructField("year", IntegerType(), True),
                StructField("quarter", IntegerType(), True),
                StructField("month", IntegerType(), True),
                StructField("month_name", StringType(), True),
                StructField("day", IntegerType(), True),
                StructField("day_of_week", IntegerType(), True),
                StructField("day_name", StringType(), True),
                StructField("week_of_year", IntegerType(), True),
                StructField("is_weekend", BooleanType(), True),
            ])
        self.spark = spark_session
    
    def initial_load(self):
        """
            Create the dim_date dimension table from seed data.
        """
        try:
            logger.info(f"{favicon['info']} Starting dim_date transformation")

            date_df = self.spark.read.format('csv') \
                                .schema(self.schema) \
                                .option('header', True) \
                                .option('mergeSchema', True) \
                                .load(config['seeds']['dim_date']['path'])
            
            logger.info(f"{favicon['right']} dim_date transformation completed successfully")
            return date_df

        except Exception as e:
            logger.error(f"{favicon['error']} Error during dim_date transformation: %s", str(e))
            return None
        
        
    def incremental_load(self):
        """Incrementally load the dim_date dimension table (not yet implemented).

        Checks if dim_date already exists in S3. If not found, performs initial load.
        If found, compares against new dates in seed files and appends any new dates
        to the existing dim_date table.
        """
        pass