import os
import sys
import yaml
from delta.tables import DeltaTable
from urllib.parse import urlparse
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
from src.common.loader import Loader
from src.common.aws import S3Util
from src.silver.base import Model
from src.silver.validation import Test

load_dotenv()
logger = get_logger()

s3_util = S3Util()

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

with open('datasets.yml', 'r') as file:
    dataset_config = yaml.safe_load(file)

with open('./src/silver/facts.yml', 'r') as file:
    fact_config = yaml.safe_load(file)


# Silver fact model for the yellow taxi dataset.
class FactYellowTrip(Model):
    """Build and maintain the yellow taxi silver fact table."""

    def __init__(self):
        """Initialize the model with its target destination path and logical name."""
        self.refresh_type = 'full'
        self.destination = rf"s3a://{bucket_name}/{layer}/{fact_name}/"


    def transform(self, required_dimensions, df):
        """Curate a bronze dataframe into the silver fact schema.

        The method filters out invalid future rows, generates a stable trip identifier,
        enriches the data with surrogate keys from dimension tables, and reshapes the
        fields into the fact-table structure used by the silver layer.

        Args:
            spark: Active Spark session.
            df: Input dataframe from the bronze layer.

        Returns:
            DataFrame: Curated dataframe ready for silver storage.
        """

        # Load the dimensional lookup tables needed for surrogate-key enrichment.
        logger.info(f"{favicon['info']} Loading all the dimensions for Surrogate Keys")

        dim_vendor = required_dimensions['dim_vendor'].select('vendor_id', 'vendor_sk')
        dim_rate_code = required_dimensions['dim_rate_code'].select('rate_code_id', 'rate_code_sk')
        dim_payment_method = required_dimensions['dim_payment_method'].select('payment_method_id', 'payment_method_sk')
        dim_location = required_dimensions['dim_location'].select('location_id', 'location_sk')
        
        fact_yellowtrip_df = df
        # prefiltering data to eliminate any future rows or rows from ery past
        fact_yellowtrip_df = fact_yellowtrip_df.filter(
                (to_date(col('tpep_pickup_datetime')) >= to_date(lit('2015-01-01'), 'yyyy-MM-dd')) & \
                (to_date(col('tpep_pickup_datetime')) <= current_date())
            )
        
        # Create a stable business key for each trip record.
        trip_id_hash_candidates = fact_yellowtrip_df.columns
        fact_yellowtrip_df =fact_yellowtrip_df.withColumn(
            'trip_id', sha2(concat_ws("||", *[col(c) for c in trip_id_hash_candidates]), 256)
        )

        fact_yellowtrip_df = fact_yellowtrip_df.dropDuplicates()

        logger.info(f"{favicon['info']} Transforming fact table to include surrogate keys from dimensions")
        fact_yellowtrip_df = fact_yellowtrip_df \
                .join(broadcast(dim_vendor), on=fact_yellowtrip_df['VendorID'] == dim_vendor['vendor_id'], how='left') \
                .join(broadcast(dim_rate_code), on=fact_yellowtrip_df['RateCodeID'] == dim_rate_code['rate_code_id'], how='left') \
                .join(broadcast(dim_payment_method), on=fact_yellowtrip_df['payment_type'] == dim_payment_method['payment_method_id'], how='left') \
                .join(broadcast(dim_location).alias('pu_location'), on=(fact_yellowtrip_df['PULocationID'] == col('pu_location.location_id')), how='left') \
                .join(broadcast(dim_location).alias('do_location'), on=(fact_yellowtrip_df['DOLocationID'] == col('do_location.location_id')), how='left') \
                .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
                .withColumn('year', expr('YEAR(pickup_datetime)')) \
                .withColumn('month', expr('MONTH(pickup_datetime)')) \
                .withColumn('day', expr('DAY(pickup_datetime)')) \
                .withColumn('partition_day', expr('TO_DATE(pickup_datetime)')) \
                .select(
                    'trip_id',
                    'vendor_sk', 
                    'pickup_datetime',
                    'dropoff_datetime',
                    'passenger_count',
                    'trip_distance',
                    'rate_code_sk',
                    'store_and_fwd_flag',
                    col('pu_location.location_sk').alias('pu_location_sk'),
                    col('do_location.location_sk').alias('do_location_sk'),
                    'payment_method_sk',
                    'fare_amount',
                    'extra',
                    'mta_tax',
                    'tip_amount',
                    'tolls_amount',
                    'improvement_surcharge',
                    'total_amount',
                    'congestion_surcharge',
                    'airport_fee',
                    'year',
                    'month',
                    'day',
                    'partition_day'
                )
        
        return fact_yellowtrip_df
                                            
    

    def initiate_transform(self, spark):
        """Build the silver fact table from every successfully ingested bronze file.

        Args:
            spark: Active Spark session used to read bronze data and write the Delta table.
        """
        database_obj = Database()
        ###################################################################################
        # load required datasets
        ###################################################################################
        required_dimensions = {}
        required_partitions = []

        for dataset in depends_on['silver']['dimensions']:
            dataset_path = dataset_config['datasets'][dataset]['path']
            dataset_format = dataset_config['datasets'][dataset]['format']
            required_dimensions[dataset] = spark.read.format(dataset_format).load(dataset_path)

        for dataset in depends_on['bronze']:
            path = dataset_config['datasets'][dataset]['path']
            parsed = urlparse(path)
            prefix = parsed.path.lstrip("/")
            required_partitions = s3_util.get_partitions(bucket_name=bucket_name, prefix=prefix)

        required_partitions = sorted(required_partitions)
        # Load all bronze files that were successfully ingested and build the silver fact table.
        if self.refresh_type == 'full':
            for partition in required_partitions:
                logger.info(f"{favicon['info']} Building fact from file {partition}")
                df = spark.read.parquet(f"s3a://{bucket_name}/{partition}")
                fact_yellowtrip_df = self.transform(required_dimensions, df)                

                kwargs = {
                    'dataframe': fact_yellowtrip_df,
                    'file_name': fact_name,
                    'source': partition,
                    'destination': self.destination,
                    'mode': 'append',
                    'partition_column': ','.join([partition_col for partition_col in partition_columns]),
                    'format': table_format,
                    'layer': layer
                }
                
                yield kwargs
        
        elif self.refresh_type == 'incremental':
            file_path = dataset_config['datasets'][depends_on['bronze'][0]]['path']
            ##############################################################################################################
            # Reading Last Two months of data from bronze layer
            # This is done to prevent file not found exception in case latest partition is not present in datalake
            ##############################################################################################################     
            fact_yellowtrip_df_new_records = spark.read.parquet(file_path) \
                    .filter(
                        col('tpep_pickup_datetime') >= expr("date_trunc('month', add_months(current_date(), -2))")
                    )
            
            fact_yellowtrip_df_new_records = self.transform(required_dimensions, fact_yellowtrip_df_new_records)
            fact_yellowtrip_delta_table = DeltaTable.forPath(spark, self.destination)
            
            # please write TARGET and SOURCE in capitals
            kwargs = {
                'source_df': fact_yellowtrip_df_new_records,
                'target_df': fact_yellowtrip_delta_table,
                'condition': f"""
                    TARGET.trip_id = SOURCE.trip_id,
                    AND TARGET.partition_day = SOURCE.partition_day 
                    """,
                'file_name': fact_name,
                'source': file_path,
                'destination': self.destination,
                'method': 'merge',
                'layer': layer
            }

            yield kwargs
        
        else:
            logger.error(f"""{favicon['error']} Please set correct Load Type at the fact.yml. 
                         Allowed Values initial(for full refresh)/incremental(for incremental refreshes)""")


if __name__ == '__main__':
    ####################################
    #  Defining config Variables
    ####################################
    # Load the silver fact configuration and target storage layer settings.
    layer = config['layers']['silver']
    bucket_name = config['storage']['bucket_name']
    loader = Loader()
    
    fact_name = os.path.abspath(__file__).split('/')[-1].split('.')[0]
    table_format = fact_config['facts'][fact_name]['format']
    partition_columns = fact_config['facts'][fact_name]['partition_columns']
    depends_on = fact_config['facts'][fact_name]['depends_on']

    # # Creating Spark Manager
    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()
    logger.info(f"{favicon['info']} Building {fact_name}...")
    yt = FactYellowTrip()

    for kwargs in yt.initiate_transform(spark=spark):
        if yt.refresh_type == 'full':
            loader.load_dataframe(**kwargs)
        else:
            loader.incremental_load(**kwargs)
    
    ######################################################
    # start Testing
    ######################################################
    testing_obj = Test()
    test_result = testing_obj.perform_tests(spark=spark, fact_name=fact_name)
    logger.info(f"{favicon['right']} Successfully Built the {fact_name}")