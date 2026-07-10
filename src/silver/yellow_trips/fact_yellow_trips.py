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
from src.silver.base import Model
from repository.metadata_query import QueryStore

load_dotenv()
logger = get_logger()

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)


class YellowTrips(Model):

    def __init__(self):
        pass

    def __curate_dataset(self, spark, df):
        """
        Transform NYC taxi data from raw to processed format by adding relevant surrogate keys
        """
        # Load dimensions
        logger.info(f"{favicon['info']} Loading all the dimensions for Surrogate Keys")
        dim_vendor = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_vendors").select('vendor_id', 'vendor_sk')
        dim_rate_code = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_rate_code").select('rate_code_id', 'rate_code_sk')
        dim_payment_method = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_payment_method").select('payment_method_id', 'payment_method_sk')
        dim_location = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/dim_location").select('location_id', 'location_sk')
        
        fact_yellowtrip_df = df
        # Adding Trip Id for each row
        trip_id_hash_candidates = ['VendorID', 'RateCodeID', 'PULocationID', 'DOLocationID', 'payment_type', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
        fact_yellowtrip_df =fact_yellowtrip_df.withColumn(
            'trip_id', sha2(concat_ws("||", *[col(c) for c in trip_id_hash_candidates]), 256)
        )

        logger.info(f"{favicon['info']} Transforming Fact table to include surrogate keys from dimensions")
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
        bronze_processed_file_query = QueryStore.get_successful_bronze_files()
        database_obj = Database()
        rs = database_obj.execute(bronze_processed_file_query)
        result = rs.fetchall()

        files = [file[0] for file in result]
        
        for file in files:
            fact_yellowtrip_df = spark.read.parquet(file)
            fact_yellowtrip_df = self.__curate_dataset(spark, fact_yellowtrip_df)
            yield fact_yellowtrip_df, file

    def incremental_load(self, spark):
        get_latest_files_query = QueryStore().get_n_latest_files(3)
        database_obj = Database()
        rs = database_obj.execute(get_latest_files_query)
        sources = rs.fetchall()
        file_path = [source[0] for source in sources]
        fact_yellowtrip_df_new_records = spark.read.parquet(*file_path)
        fact_yellowtrip_df_new_records = self.__curate_dataset(spark, fact_yellowtrip_df_new_records)

        fact_yellowtrip_dfs = spark.read.parquet("s3a://nyc-traffic-spark-2026/silver/")

        for fact_yellowtrip_df in fact_yellowtrip_dfs:
            fact_yellowtrip_df.show()
            return 
        
        # return fact_yellowtrip_df


if __name__ == '__main__':
    spark_obj = SparkManager()
    spark = spark_obj.get_spark_session()

    yt = YellowTrips()

    silver = config['layers']['silver']
    bucket_name = config['storage']['bucket_name']
    db_obj = Database()

    name = 'fact_yellow_trip'
    logger.info(f"{favicon['info']} Building Fact {name}")
    s3_key = rf"s3a://{bucket_name}/{silver}/{name}/"

    dfs = yt.initial_load(spark)

    for fact_yellowtrip_df, file in dfs:
        try:
            query = QueryStore().silver_load_log(name, file, s3_key, 'UPLOADING', 'append')
            db_obj.execute(query=query)
            
            fact_yellowtrip_df.write.partitionBy('year', 'month') \
                    .mode('append') \
                    .parquet(s3_key)
        
            query = QueryStore().silver_load_log(name, file, s3_key, 'SUCCESS', 'append')
            db_obj.execute(query=query)
        
        except:
            query = QueryStore().silver_load_log(name, file, s3_key, 'FAILED', 'append')
            db_obj.execute(query=query)



    



