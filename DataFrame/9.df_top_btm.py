from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("text conversion").getOrCreate()

# Relative File path for CSV file.
file_Path = r"Docs\files\customers-100.csv"

cust_df = spark.read.option("header","True").option("inferSchema","True").csv(file_Path)

# top record
print(cust_df.first())

# Top 5 records using head()
print(cust_df.head(5))

# Top 5 records using take()
print(cust_df.take(5))

# Top 5 records using show()
cust_df.show(5)

# Last 10 records

print(cust_df.tail(10))