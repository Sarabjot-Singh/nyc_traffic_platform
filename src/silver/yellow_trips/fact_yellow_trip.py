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
fact_name = fact_config['facts']['fact_yellow_trips']['name']
silver = config['layers']['silver']
bucket_name = config['storage']['bucket_name']


# Silver fact model for the yellow taxi dataset.
class FactYellowTrips(Model):

    def __init__(self):
        self.name = __file__.split('/')[-1].split('.')[0]
        self.destination = rf"s3a://{bucket_name}/{silver}/{fact_name}/"

    def __curate_dataset(self, spark, df):
        """
        Transform NYC taxi data from raw to processed format by adding relevant surrogate keys.
        """
        # Load the dimensional lookup tables needed for surrogate-key enrichment.
        logger.info(f"{favicon['info']} Loading all the dimensions for Surrogate Keys")
        dim_vendor = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_vendors").select('vendor_id', 'vendor_sk')
        dim_rate_code = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_rate_code").select('rate_code_id', 'rate_code_sk')
        dim_payment_method = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_payment_method").select('payment_method_id', 'payment_method_sk')
        dim_location = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_location").select('location_id', 'location_sk')
        
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
                    'day'
                )
        
        return fact_yellowtrip_df
                                            
    
    def initial_load(self, spark):
        # Load all bronze files that were successfully ingested and build the silver fact table.
        bronze_processed_file_query = QueryStore.get_successful_bronze_files()
        
        database_obj = Database()
        rs = database_obj.execute(bronze_processed_file_query)
        result = rs.fetchall()

        files = [file[0] for file in result]
        
        for file in files:
            logger.info(f"{favicon['info']} Building fact from file {file}")
            fact_yellowtrip_df = spark.read.parquet(file)
            fact_yellowtrip_df = self.__curate_dataset(spark, fact_yellowtrip_df)
            try:
                query = QueryStore().silver_load_log(self.name, file, self.destination, 'UPLOADING', 'append')
                database_obj.execute(query=query)
                
                fact_yellowtrip_df \
                        .write \
                        .format('delta') \
                        .mode('overwrite') \
                        .partitionBy('year', 'month') \
                        .save(self.destination)
            
                query = QueryStore().silver_load_log(self.name, file, self.destination, 'SUCCESS', 'append')
                database_obj.execute(query=query)
                logger.info(f"{favicon['right']} Building fact from file {file}")

            except:
                logger.info(f"{favicon['error']} Failed building fact from file {file}")
                query = QueryStore().silver_load_log(self.name, file, self.destination, 'FAILED', 'append')
                database_obj.execute(query=query)


    def incremental_load(self, spark):
        # Apply the latest bronze files to the Delta fact table incrementally.
        database_obj = Database()

        #################################################################
        # Get paths of two last inserted file from Raw Layer
        #################################################################
        get_latest_files_query = QueryStore().get_n_latest_files(1)
        rs = database_obj.execute(get_latest_files_query)
        sources = rs.fetchall()
        file_path = [source[0] for source in sources]
        
        ##############################################################################################################
        # Reading Last Two months of data from raw layer
        # This is done to prevent file not found exception in case latest partition is not present in datalake
        ##############################################################################################################     
        fact_yellowtrip_df_new_records = spark.read.parquet(*file_path)
        fact_yellowtrip_df_new_records = fact_yellowtrip_df_new_records.filter(
                (to_date(col('tpep_pickup_datetime')) >= to_date(lit('2015-01-01'), 'yyyy-MM-dd')) & \
                (to_date(col('tpep_pickup_datetime')) <= current_date())
            )
        fact_yellowtrip_df_new_records = self.__curate_dataset(spark, fact_yellowtrip_df_new_records)

        fact_yellowtrip_delta_table = DeltaTable.forPath(spark, self.destination)

        logger.info(f"{favicon['info']}")
        fact_yellowtrip_delta_table.alias("TARGET").merge(
            source=fact_yellowtrip_df_new_records.alias("SOURCE"),
            condition="""
                TARGET.trip_id = SOURCE.trip_id 
                AND TARGET.year = SOURCE.year 
                AND TARGET.month = SOURCE.month
            """ 
        ) \
        .withSchemaEvolution() \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        return


if __name__ == '__main__':
    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()
    logger.info(f"{favicon['info']} Building {fact_name}...")
    yt = FactYellowTrips()

    yt.incremental_load(spark)

    logger.info(f"{favicon['right']} Successfully Built the {fact_name}")