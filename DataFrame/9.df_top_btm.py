from pyspark.sql import SparkSession
from pyspark import SparkContext as sc

# Creating SparkSession
spark = SparkSession.builder.appName("text conversion").getOrCreate()

# Getting SparkContext from SparkSession
sc = spark.sparkContext 

# Relative File path for CSV file.
file_Path = r"Docs\files\customers-100.csv"
cust_df = spark.read.option("header","True").option("inferSchema","True").csv(file_Path)

# Top record or First Record
print("\nOnly showing first record from top\n")
first_rec = [cust_df.first()]
df_first = spark.createDataFrame(first_rec)
df_first.show()

# Top 5 records using head()
print("\nShowing Top 5 records from top Using head()\n")
head_rec = cust_df.head(5)
df_head = spark.createDataFrame(head_rec,schema=cust_df.schema)
df_head.show() 

# Top 5 records using take()
print("\nShowing Top 5 records from top Using take()\n")
take_rec = cust_df.take(5)
rdd = sc.parallelize(take_rec)
df_take = rdd.toDF()
df_take.show()

# Top 5 records using show()
print("\nShowing Top 5 records from top Using show()\n")
cust_df.show(5)

# Last 10 records
print("\nShowing Last 10 records Using tail()\n")
tail_rec = cust_df.tail(10)
rdd = sc.parallelize(tail_rec)
df_tail = rdd.toDF()
df_tail.show()