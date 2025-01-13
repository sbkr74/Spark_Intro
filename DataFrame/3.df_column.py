from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('column').getOrCreate()

df = spark.read.format("csv") \
    .option("path",r"Docs\files\customers-100.csv") \
    .option("header","True") \
    .load()

df1 = df.select('customer id','country','subscription date')

# adding Column
from pyspark.sql.functions import lit,col,date_add,dayofmonth
df2 = df1.withColumn('destination',lit("India"))
df2.show(n=5)

# adding column with expression
df3 = df1.withColumn('new_date',date_add(col('subscription date'),2)).withColumn("Earned_Sal",dayofmonth(col('subscription date')))
df3.show(n=5)

# printing Schema of dataframe
df3.printSchema()

# modify existing column
df4 = df3.withColumn('subscription date',col('subscription date').cast("date"))
df4.printSchema()

# adding Column based on condition
