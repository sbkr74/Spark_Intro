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
from pyspark.sql.functions import when
data_df = spark.read.format("csv") \
    .option("path",r"Docs\files\014-Data.csv") \
    .option("header","True") \
    .load()

data_df=data_df.withColumn("category",when(col("Color")=="Red",1).otherwise(col("Color")))
data_df.show(n=20)

# for multiple conditions
data_df_mult = data_df.withColumn("category",when(col("Color")=="Red",1).when(col("Color")=="Silver",2).when(col("Color")=="Black",3).otherwise(col("Color")))
data_df_mult.show(10)

# drop columns 
drop_df = data_df_mult.drop("Country")
drop_df.show(n=20)

# multiple drop
mult_drop_df = data_df_mult.drop("ProductName","ListPrice","category")
mult_drop_df.show(n=25)

# Renaming Column
ren_df = data_df.withColumnRenamed("SalesAmount","Sales")
ren_df.printSchema()

# Renaming Multiple Column
mul_ren_df = data_df.withColumnRenamed("ProductKey","ProductId").withColumnRenamed("TaxAmt","Tax")
mul_ren_df.printSchema()

# Renaming using select Expr (but all needed column to be passed to display)
ren_df_sel = data_df.selectExpr("ProductKey as ProductID","SalesAmount","CustomerName as Name","Country")
ren_df_sel.show(n=5)

print(data_df.columns)
print("\n------------------------------------------------------------------------------\n")
new_col = list(map(lambda x: x.upper(),data_df.columns))
print(new_col)
ren_df_upper = data_df.toDF(*new_col)
ren_df_upper.show(n=3)