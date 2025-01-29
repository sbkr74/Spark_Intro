from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('filter operation').getOrCreate()

file_path = r"Docs\files\person_data_quality.csv"
df = spark.read.option("Header","True").option("inferSchema","True").csv(file_path)

df_top = df.limit(10)
df_bottom = df.orderBy("Record ID",ascending=False).limit(5)

df_union = df_top.union(df_bottom)
df_union.show()

