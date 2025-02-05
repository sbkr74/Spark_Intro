from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating SparkSession
spark = SparkSession.builder \
            .appName("Agg. function") \
            .getOrCreate()

# CSV File path 
file_path = "Docs/files/014-Data.csv"

df = spark.read.option("header","True") \
        .option("inferSchema","True") \
        .csv(file_path)

# Use of Agg functions
# Use of sum()
df_sum = df.select(sum("SalesAmount"))
df_sum.show(5)

# Use of sum() with alias
df_sum = df.select(sum("SalesAmount").alias("TotalSales"))
df_sum.show(5)