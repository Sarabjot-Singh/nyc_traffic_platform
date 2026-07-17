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
from src.common.database import Database
from src.gold.base import Model
from repository.metadata_query import QueryStore

load_dotenv()
logger = get_logger()

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

with open('./src/gold/gold_datasets.yml', 'r') as gold_config_file:
    gold_config = yaml.safe_load(gold_config_file)

class TblYellowTripsDashboard():

    def __init__(self):
        """Initialize the model with its target destination path and logical name."""
        self.name = __file__.split('/')[-1].split('.')[0]
        self.destination = rf"s3a://{bucket_name}/{gold}/{dataset_name}/"

    def transform(self, spark, df):
        # Load the dimensional lookup tables needed for surrogate-key enrichment.
        logger.info(f"{favicon['info']} Transforming dataset")
        dim_vendor = spark.read.format('delta').load("s3a://nyc-traffic-spark-2026/silver/dim_vendors")
        dim_rate_code = spark.read.format('delta').load("s3a://nyc-traffic-spark-2026/silver/dim_rate_code")
        dim_payment_method = spark.read.format('delta').load("s3a://nyc-traffic-spark-2026/silver/dim_payment_method")
        dim_location = spark.read.format('delta').load("s3a://nyc-traffic-spark-2026/silver/dim_location")
        
        fact_yellowtrip_df = df

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
        

        fact_yellowtrip_df = spark.read.format('delta').load("s3a://nyc-traffic-spark-2026/silver/fact_yellow_trip/").filter(
                col('partition_day') >= to_date(lit('2021-01-01'))
            )

        kwargs = {
            'dataframe': fact_yellowtrip_df,
            'file_name': dataset_name,
            'source': '',
            'destination': self.destination,
            'method': load_mode,
            'load_type': load_type
        }





if __name__ == '__main__':
    ####################################
    #  Defining config Variables
    ####################################
    # Load the silver fact configuration and target storage layer settings.
    file_name = os.path.abspath(__file__).split('/')[-1].split('.')[0]
    gold = config['layers']['gold']
    silver = config['layers']['gold']
    bucket_name = config['storage']['bucket_name']

    source_name = gold_config['datasets'][file_name]['source_bronze_name']
    dataset_name = gold_config['datasets'][file_name]['module']
    load_mode = gold_config['datasets'][file_name]['mode']
    load_type = gold_config['datasets'][file_name]['load_type']
    table_format = gold_config['datasets'][file_name]['table_format']
    partition_column = gold_config['datasets'][file_name]['partition_column']

    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()
    # logger.info(f"{favicon['info']} Building {fact_name}...")
    
    yellow_trips_dashboard = TblYellowTripsDashboard()
    yellow_trips_dashboard.transform(spark=spark)

