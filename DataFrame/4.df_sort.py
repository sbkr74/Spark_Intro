from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('sorted_DF') \
        .getOrCreate()

df = spark.read \
        .option("header","True") \
        .option("inferSchema","True") \
        .csv(r"Docs\files\data.csv")

# Single column sorting
sorted_df = df.sort("CustomerName")
sorted_df.show()

# multiple column sorting
mult_sorted_df = df.sort("ProductKey","CustomerName")
mult_sorted_df.show()

# using col() for better sorting
from pyspark.sql.functions import col

# single column sorting using col()
df_col_sort = df.sort(col("ProductKey"))
df_col_sort.show()

# multiple column sorting using col()
df_mult_col_sort = df.sort(col("CustomerName"),col("ProductKey"))
df_mult_col_sort.show()

# single column sorting using col() in descending order
df_col_sort = df.sort(col("ProductKey").desc())
df_col_sort.show()

# multiple column sorting using col() in ascending and descending order
df_mult_col_sort = df.sort(col("CustomerName"),col("ProductKey").asc(),col('SalesAmount').desc())
df_mult_col_sort.show()