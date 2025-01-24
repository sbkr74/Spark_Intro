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

# ----------------------------------------------------------#-----------------------------------------------------#
# Null OPERATIONS
filepath = r"Docs\files\person_data_quality.csv"
df = spark.read.option("Header","True").option("inferSchema","True").csv(filepath)
df.show()

# for implementing int value for int column having null value
df_num = df.na.fill(0)
df_num.show()

# for implementing string value for string column having null value
df_alpha = df.na.fill("N/A")
df_alpha.show()

# To fill null values for both types of column
df_both = df.na.fill("N/A").na.fill(0)
df_both.show()

# Using expr() for single column
from pyspark.sql.functions import expr
df_expr = df.withColumn("Age",expr("coalesce(Age,0)"))
df_expr.show()

# Using expr() for multiple column
from pyspark.sql.functions import expr
df_expr_mult = df.withColumn("Name",expr("coalesce(`Name`,'N/A')")).withColumn("Email",expr("coalesce(`Email`,'not registered')"))
df_expr_mult.show()