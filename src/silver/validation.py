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

with open('datasets.yml', 'r') as file:
    dataset_config = yaml.safe_load(file)

with open('./src/silver/facts.yml', 'r') as fact_config_file:
    fact_config = yaml.safe_load(fact_config_file)

####################################
#  Defining config Variables
####################################
# Load the silver fact configuration and target storage layer settings.
bronze = config['layers']['bronze']
silver = config['layers']['silver']
bucket_name = config['storage']['bucket_name']

class Test:

    def __validate_row_count(self, spark, fact_name):
        depends_on = fact_config['facts'][fact_name]['depends_on']
        source_bronze_name = depends_on['bronze'][0]
        bronze_data_path = dataset_config['datasets'][source_bronze_name]['path']
        silver_fact_path = f"s3a://{bucket_name}/{silver}/{fact_name}/"

        fact_yellowtrip_df_bronze = spark.read.parquet(bronze_data_path)
        
        bronze_count = fact_yellowtrip_df_bronze.count()
        fact_yellowtrip_df_silver = spark.read.format('delta').load(silver_fact_path)
        silver_count = fact_yellowtrip_df_silver.count()

        print(bronze_count)
        print(silver_count)

        return bronze_count >= silver_count
    

    def perform_tests(self, spark, fact_name):
        logger.info(f"{favicon['info']} Performing Row Count Check")
        row_count_check = self.__validate_row_count(spark, fact_name)
        if row_count_check:
            logger.info(f"{favicon['right']} Performing Row Count Check")
        else:
            logger.info(f"{favicon['error']} Failed Check - Row Count Check")