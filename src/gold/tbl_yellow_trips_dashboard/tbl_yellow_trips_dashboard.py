import os
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
from src.common.loader.loader import Loader
from src.common.database import Database
from src.gold.base import Model
from repository.metadata_query import QueryStore

load_dotenv()
logger = get_logger()

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

with open('datasets.yml', 'r') as file:
    dataset_config = yaml.safe_load(file)

with open('./src/gold/gold_datasets.yml', 'r') as gold_config_file:
    gold_config = yaml.safe_load(gold_config_file)

class TblYellowTripsDashboard():

    def __init__(self):
        """Initialize the model with its target destination path and logical name."""
        self.name = __file__.split('/')[-1].split('.')[0]
        self.destination = rf"s3a://{bucket_name}/{gold}/{table_name}/"

    def transform(self, spark, required_dimensions, required_facts):
        # Load the dimensional lookup tables needed for surrogate-key enrichment.
        logger.info(f"{favicon['info']} Transforming dataset")
        dim_vendor = required_dimensions['dim_vendor']
        dim_rate_code = required_dimensions['dim_rate_code']
        dim_payment_method = required_dimensions['dim_payment_method']
        dim_location = required_dimensions['dim_location']
        
        fact_yellowtrip_df = required_facts['fact_yellow_trip']

        ####################################################
        # Registering temporary views to use spark SQL
        ####################################################
        
        dim_vendor.createOrReplaceTempView('dim_vendor')
        dim_rate_code.createOrReplaceTempView('dim_rate_code')
        dim_payment_method.createOrReplaceTempView('dim_payment_method')
        dim_location.createOrReplaceTempView('dim_location')

        fact_yellowtrip_df.createOrReplaceTempView('fact_yellowtrip_df')

        tbl_yellow_trips_dashboard = spark.sql(
            """
                SELECT 
                    partition_day AS activity_date,
                    dlpu.borough as pickup_borough,
                    dldo.borough as dropoff_borough,
                    dim_vendor.description AS vendor,
                    dim_rate_code.description AS rate_code,
                    dim_payment_method.description AS payment_method,
                    SUM(timestampdiff(SECOND, pickup_datetime, dropoff_datetime)) AS total_trip_duration_in_seconds,
                    COUNT(trip_id) AS total_trips,
                    SUM(trip_distance) AS total_distance,
                    SUM(fare_amount) AS total_fare_amount,
                    SUM(tip_amount) AS total_trip_amount,
                    SUM(total_amount) AS total_amount,
                    SUM(passenger_count) AS total_passenger
                FROM fact_yellowtrip_df
                LEFT JOIN dim_vendor 
                    ON fact_yellowtrip_df.vendor_sk = dim_vendor.vendor_sk
                LEFT JOIN dim_rate_code 
                    ON fact_yellowtrip_df.rate_code_sk = dim_rate_code.rate_code_sk
                LEFT JOIN dim_payment_method 
                    ON fact_yellowtrip_df.payment_method_sk = dim_payment_method.payment_method_sk
                LEFT JOIN dim_location AS dlpu
                    ON fact_yellowtrip_df.pu_location_sk = dlpu.location_sk
                LEFT JOIN dim_location AS dldo
                    ON fact_yellowtrip_df.do_location_sk = dldo.location_sk
                GROUP BY 1, 2, 3, 4, 5, 6
            """
        )
        
        return tbl_yellow_trips_dashboard
                                            
    
    def initiate_transform(self, spark):
        # """Build the silver fact table from every successfully ingested bronze file.

        # Args:
        #     spark: Active Spark session used to read bronze data and write the Delta table.
        # """
        # Load all bronze files that were successfully ingested and build the silver fact table.
        

        logger.info(f"{favicon['info']} Loading all the dimensions for Surrogate Keys")
        required_dimensions = {}
        required_facts = {}

        for dataset in depends_on['silver']['dimensions']:
            dataset_path = dataset_config['datasets'][dataset]['path']
            dataset_format = dataset_config['datasets'][dataset]['format']
            required_dimensions[dataset] = spark.read.format(dataset_format).load(dataset_path)

        for dataset in depends_on['silver']['facts']:
            dataset_path = dataset_config['datasets'][dataset]['path']
            dataset_format = dataset_config['datasets'][dataset]['format']
            required_facts[dataset] = spark.read.format(dataset_format).load(dataset_path)

        
        df = self.transform(spark=spark, required_dimensions=required_dimensions, required_facts=required_facts)        

        kwargs = {
            'dataframe': df,
            'file_name': table_name,
            'source': '',
            'destination': self.destination,
            'mode': write_mode,
            'load_type': 'full',
            'partition_column': ','.join([partition_col for partition_col in partition_columns]),
            'format': table_format,
            'log_table_name': 'metadata.silver_ingestion_log'
        }

        return kwargs

if __name__ == '__main__':
    ####################################
    #  Defining config Variables
    ####################################
    # Load the silver fact configuration and target storage layer settings.
    file_name = os.path.abspath(__file__).split('/')[-1].split('.')[0]
    gold = config['layers']['gold']
    silver = config['layers']['gold']
    bucket_name = config['storage']['bucket_name']

    table_name = os.path.abspath(__file__).split('/')[-1].split('.')[0]
    table_format = gold_config['datasets'][table_name]['format']
    write_mode = gold_config['datasets'][table_name]['mode']
    partition_columns = gold_config['datasets'][table_name]['partition_columns']
    depends_on = gold_config['datasets'][table_name]['depends_on']

    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()
    # logger.info(f"{favicon['info']} Building {fact_name}...")
    
    yellow_trips_dashboard = TblYellowTripsDashboard()

    kwargs = yellow_trips_dashboard.initiate_transform(spark=spark)

    loader = Loader()
    loader.load_dataframe(**kwargs)

