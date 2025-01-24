from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Operations').getOrCreate()
filepath = r"Docs\files\data.csv"
df = spark.read.option("Header","True").csv(filepath)
df.show(n=5)

# To find distinct records in all the fields
df_distinct = df.distinct()
df_distinct.show(n=5)

# Using Drop Duplicates 
df_drop_dp = df.dropDuplicates()
df_drop_dp.show(n=5)

# Using Drop Duplicates for column wise
df_drop_dp = df.dropDuplicates(['CustomerName'])
df_drop_dp.show(n=5)

# Using Drop Duplicates multiple column wise
df_drop_dp = df.dropDuplicates(['ProductKey','CustomerName'])
df_drop_dp.show(n=5)

# if want to see only uniques values of column
df_unique = df.select('SalesAmount').distinct()
df_unique.show(n=100)