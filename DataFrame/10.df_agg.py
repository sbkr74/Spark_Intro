from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,min,max,count,countDistinct

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
df_sum.show()

# Use of sum() with alias
df_sum = df.select(sum("SalesAmount").alias("TotalSales"))
df_sum.show()

# Use of avg()
df_avg = df.select(avg("SalesAmount"))
df_avg.show()

# Use of avg() with round and alias
df_avg = df.select(round(avg("SalesAmount"),2).alias("AvgSales"))
df_avg.show()

# Use of min()
df_min = df.select(min("SalesAmount"))
df_min.show()

# Use of min() with alias
df_min = df.select(min("SalesAmount").alias("MinSales"))
df_min.show()

# Use of max()
df_max = df.select(max("SalesAmount"))
df_max.show()

# Use of max() with alias
df_max= df.select(max("SalesAmount").alias("MaxSales"))
df_max.show()

# Use of count()
df_count = df.select(count("SalesAmount"))
df_count.show()

# Use of count() with alias
df_count_dist = df.select(count("SalesAmount").alias("NumOfSales"))
df_count_dist.show()

# Use of countDistinct() with alias
df_count_dist = df.select(countDistinct("Country").alias("UniqueCont"))
df_count_dist.show()