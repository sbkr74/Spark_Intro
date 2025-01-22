from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('sorted_DF') \
        .getOrCreate()

df = spark.read \
        .option("header","True") \
        .option("inferSchema","True") \
        .csv(r"Docs\files\data.csv")

df.show(n=10)