from pyspark.sql import SparkSession

dataPath = "./data/yellow_tripdata_2025-01.parquet"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Taxi Cab Data ETL Pipeline") \
    .getOrCreate()

df = spark.read.parquet(dataPath)
df.show()
df.printSchema()

spark.stop()