from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CSV file Read').getOrCreate()

df = spark.read.format("csv") \
    .option("path",r"Docs\files\customers-100.csv") \
    .option("header","True") \
    .load()

#########################
# Basic Operation
# select() ---> to get particular columns from dataframe.
df1 = df.select("customer id","subscription date","website")
df1.show(n=5)

# use expr()
from pyspark.sql.functions import expr
df1 = df.select("customer id","subscription date","website",expr("date_add(`Subscription Date`,3) as `ext subs`"))
df1.show(n=5)

# col() --> 
from pyspark.sql.functions import col
df2 = df.select(col("Customer ID"),col("Subscription Date"),col("Website"))
df2.show(n=5)

# selectExpr() --> use both column and expression
df3 = df.selectExpr("`Customer Id`","`Subscription Date`","Website")
df3.show(n=5)

df4 = df.selectExpr("`customer id`","`Subscription Date`","website","date_add(`Subscription Date`,3) as `ext subs`")
df4.show(n=5)