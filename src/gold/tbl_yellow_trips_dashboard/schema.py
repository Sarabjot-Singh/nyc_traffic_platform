from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    LongType,
    DoubleType
)

schema = StructType([
    StructField("pickup_date", DateType(), True),
    StructField("pickup_borough", StringType(), True),
    StructField("dropoff_borough", StringType(), True),
    StructField("vendor", StringType(), True),
    StructField("rate_code", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("total_trip_duration_in_seconds", LongType(), True),
    StructField("total_trips", LongType(), False),
    StructField("total_distance", DoubleType(), True),
    StructField("total_fare_amount", DoubleType(), True),
    StructField("total_trip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("total_passenger", LongType(), True),
])