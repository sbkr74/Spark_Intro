from pyspark.sql import SparkSession
from pyspark.sql.functions import col,upper,lower,initcap

spark = SparkSession.builder.appName("text conversion").getOrCreate()
file_Path = r"Docs\files\data.csv"
df = spark.read.option("header","True").option("inferSchema","True").csv(file_Path)

# For UpperCase
df_upper = df.withColumn("CustomerName",upper(col("CustomerName")))
df_upper.show()

# For LowerCase
df_lower = df.withColumn("CustomerName",lower(col("CustomerName")))
df_lower.show()

# For Initial Capital (initcap)
df_inCap = df_lower.withColumn("CustomerName",initcap(col("CustomerName")))
df_inCap.show()