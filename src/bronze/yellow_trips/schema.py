from pyspark.sql.types import *

schema = {
    "VendorID": LongType(),
    "tpep_pickup_datetime": TimestampNTZType(),
    "tpep_dropoff_datetime": TimestampNTZType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "RatecodeID": LongType(),
    "store_and_fwd_flag": StringType(),
    "PULocationID": LongType(),
    "DOLocationID": LongType(),
    "payment_type": LongType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "cbd_congestion_fee": DoubleType(),
}