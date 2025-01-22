from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('sorted_DF') \
        .getOrCreate()

df = spark.read \
    .option('header',"True") \
    .option("inferSchema","True") \
    .csv(r"Docs\files\014-Data.csv")

df.show()

selected_col_df = df.select('ProductKey','CustomerName','SalesAmount')
top_500_df = selected_col_df.limit(500)

top_500_df.write \
    .option("header","True") \
    .csv(r"Docs\files\data.csv")

