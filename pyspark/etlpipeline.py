from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import when, col, unix_timestamp

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

# Cleaning the data to remove negative numbers. Possible recovery of incorrect data.
df_clean = df \
    .withColumn("passenger_count", spark_abs(df["passenger_count"])) \
    .withColumn("trip_distance", spark_abs(df["trip_distance"])) \
    .withColumn("fare_amount", spark_abs(df["fare_amount"])) \
    .withColumn("extra", spark_abs(df["extra"])) \
    .withColumn("mta_tax", spark_abs(df["mta_tax"])) \
    .withColumn("tip_amount", spark_abs(df["tip_amount"])) \
    .withColumn("tolls_amount", spark_abs(df["tolls_amount"])) \
    .withColumn("improvement_surcharge", spark_abs(df["improvement_surcharge"])) \
    .withColumn("total_amount", spark_abs(df["total_amount"])) \
    .withColumn("congestion_surcharge", spark_abs(df["congestion_surcharge"])) \
    .withColumn("Airport_fee", spark_abs(df["Airport_fee"])) \
    .withColumn("cbd_congestion_fee", spark_abs(df["cbd_congestion_fee"]))


# Filters unrealistic numeric amounts
df_clean = df_clean.filter(
    (df_clean["passenger_count"] <= 6) &
    (df_clean["trip_distance"] <= 50) &
    (df_clean["fare_amount"] <= 200) &
    (df_clean["extra"] <= 20) &
    (df_clean["mta_tax"] <= 20) &
    (df_clean["tip_amount"] <= 100) &
    (df_clean["tolls_amount"] <= 120) &
    (df_clean["improvement_surcharge"] <= 1) &
    (df_clean["total_amount"] <= 310) &
    (df_clean["congestion_surcharge"] <= 2.5) &
    (df_clean["Airport_fee"] <= 7) &
    (df_clean["cbd_congestion_fee"] <= 1)
)

df_clean = df_clean.filter(df_clean["RatecodeID"] != 99)

# Filters unreasonable time
df_clean = df_clean.filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))

# Creates Gets trip duration column
df_clean = df_clean.withColumn("trip_duration_seconds", unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))

# Updates is_cargo trip if trip distance is 0
df_clean = df_clean.withColumn("is_cargo", when(col("trip_distance") == 0, True).otherwise(False))

# Writes to a single .parquet file
df_clean.coalesce(1).write.mode("overwrite").parquet("cleaned_yellow_tripdata.parquet")
