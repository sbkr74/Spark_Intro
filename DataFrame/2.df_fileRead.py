from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CSV file Read').getOrCreate()

df = spark.read.format("csv") \
    .option("path",r"Docs\files\customers-100.csv") \
    .option("header","True") \
    .load()

df.show()