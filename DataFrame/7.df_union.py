from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('filter operation').getOrCreate()

file_path = r"Docs\files\person_data_quality.csv"
df = spark.read.option("Header","True").option("inferSchema","True").csv(file_path)

df_top = df.limit(10)
df_bottom = df.orderBy("Record ID",ascending=False).limit(5)

df_union = df_top.union(df_bottom)
df_union.show()

# If column names are not aligned
df_union = df_top.unionByName(df_bottom)
df_union.show()

df1 = df.select("Name","Age","Email").limit(10)
df2 = df.select("Age","Name","Email").orderBy(col("Record Id").desc()).limit(5)

df_union = df1.union(df2)           # problem with union if column names are not aligned
df_union.show()

df_union = df1.unionByName(df2)           
df_union.show()