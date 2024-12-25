from pyspark.sql import SparkSession



# Define data as a list of dictionaries
data = [
    {'Name': 'Alice', 'Age': 25, 'City': 'New York'},
    {'Name': 'Bob', 'Age': 30, 'City': 'Los Angeles'},
    {'Name': 'Charlie', 'Age': 35, 'City': 'Chicago'}
]
spark = SparkSession.builder.appName('Basic_Dataframe').getOrCreate()
df = spark.createDataFrame(data)
df.show()

# Stop Spark
spark.stop()