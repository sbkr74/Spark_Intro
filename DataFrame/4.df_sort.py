from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('sorted_DF') \
        .getOrCreate()

df = spark.read \
        .option("header","True") \
        .option("inferSchema","True") \
        .csv(r"Docs\files\data.csv")

# Single column sorting
sorted_df = df.sort("CustomerName")
sorted_df.show()

# multiple column sorting
mult_sorted_df = df.sort("ProductKey","CustomerName")
mult_sorted_df.show()