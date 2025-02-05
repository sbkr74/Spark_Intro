from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,round

# Creating SparkSession
spark = SparkSession.builder \
            .appName("Agg. function") \
            .getOrCreate()

# CSV File path 
file_path = "Docs/files/014-Data.csv"

df = spark.read.option("header","True") \
        .option("inferSchema","True") \
        .csv(file_path)

df_grp = df.groupBy("Country").sum("SalesAmount").withColumnRenamed("sum(SalesAmount)", "TotalSales")
df_grp.show()

