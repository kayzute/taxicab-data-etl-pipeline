from pyspark.sql import SparkSession

#  |-- VendorID: integer (nullable = true)
#  |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
#  |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
#  |-- passenger_count: long (nullable = true)
#  |-- trip_distance: double (nullable = true)
#  |-- RatecodeID: long (nullable = true)
#  |-- store_and_fwd_flag: string (nullable = true)
#  |-- PULocationID: integer (nullable = true)
#  |-- DOLocationID: integer (nullable = true)
#  |-- payment_type: long (nullable = true)
#  |-- fare_amount: double (nullable = true)
#  |-- extra: double (nullable = true)
#  |-- mta_tax: double (nullable = true)
#  |-- tip_amount: double (nullable = true)
#  |-- tolls_amount: double (nullable = true)
#  |-- improvement_surcharge: double (nullable = true)
#  |-- total_amount: double (nullable = true)
#  |-- congestion_surcharge: double (nullable = true)
#  |-- Airport_fee: double (nullable = true)
#  |-- cbd_congestion_fee: double (nullable = true)

dataPath = "../data/yellow_tripdata_2025-01.parquet"

# initialize PySpark Cluster
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Taxi Cab Data ETL Pipeline") \
    .getOrCreate()

# Read parquet file
df = spark.read.parquet(dataPath)
df.show(5)
df.printSchema()
df.describe().show()

# Cleaning the data
df_clean = df.dropna() # drops rows with NaN values

# Cleans any negative amounts
df_clean = df_clean.filter(
    (df_clean["passenger_count"] > 0) &
    (df_clean["fare_amount"] > 0) &
    (df_clean["trip_distance"] > 0) &
    (df_clean["tip_amount"] >= 0) & # accounts for tips being $0
    (df_clean["total_amount"] > 0)
)

# Removes extreme data

df_clean = df_clean.filter(
    (df_clean["trip_distance"] < 100) # trips over 100 miles
)