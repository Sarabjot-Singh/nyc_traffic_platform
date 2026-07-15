import sys
import yaml
from pathlib import Path
from pyspark.sql.functions import *
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon
from src.common.spark import SparkManager
from src.common.database import Database
from repository.metadata_query import QueryStore

load_dotenv()
logger = get_logger()

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

with open('./src/silver/facts.yml', 'r') as fact_config_file:
    fact_config = yaml.safe_load(fact_config_file)

####################################
#  Defining config Variables
####################################
# Load the silver fact configuration and target storage layer settings.
raw = config['layers']['bronze']
silver = config['layers']['silver']
bucket_name = config['storage']['bucket_name']

class Test:

    def __init__(self, fact_name):
        self.fact_name = fact_name
        self.fact_config = fact_config['facts'][self.fact_name]

    def __validate_row_count(self, spark):
        source_bronze_name = fact_config['facts'][self.fact_name]['source_bronze_name']
        raw_data_path = f"s3a://{bucket_name}/{raw}/{source_bronze_name}/"
        silver_fact_path = f"s3a://{bucket_name}/{silver}/{self.fact_name}/"
        fact_yellowtrip_df_raw = spark.read.parquet(raw_data_path)
        raw_count = fact_yellowtrip_df_raw.count()

        fact_yellowtrip_df_silver = spark.read.format('delta').load(silver_fact_path)
        silver_count = fact_yellowtrip_df_silver.count()

        print(raw_count)
        print(silver_count)

        return raw_count >= silver_count
    
    def perform_tests(self, spark):
        logger.info(f"{favicon['info']} Performing Row Count Check")
        row_count_check = self.__validate_row_count(spark)
        if row_count_check:
            logger.info(f"{favicon['right']} Performing Row Count Check")
        else:
            logger.info(f"{favicon['error']} Failed Check - Row Count Check")