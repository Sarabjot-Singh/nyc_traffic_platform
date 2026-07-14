import sys
import yaml
from delta.tables import DeltaTable
from dateutil.relativedelta import relativedelta
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
from src.silver.base import Model
from src.silver.yellow_trips.schema import schema
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

    def validate_row_count(self, spark):
        raw_data_path = f"s3a://{bucket_name}/{raw}/yellow_tripdata/"
        silver_fact_path = f"s3a://{bucket_name}/{silver}/fact_yellow_trip/"
        fact_yellowtrip_df_raw = spark.read.parquet(raw_data_path)
        raw_count = fact_yellowtrip_df_raw.count()

        fact_yellowtrip_df_silver = spark.read.parquet(silver_fact_path)
        silver_count = fact_yellowtrip_df_silver.count()


        print(raw_count)
        print(silver_count)

        return raw_count == silver_count

spark_obj = SparkManager()
spark = spark_obj.get_spark_session()
test = Test('fact_yellow_trip')
test.validate_row_count(spark)
