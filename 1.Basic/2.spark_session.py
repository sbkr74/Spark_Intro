from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('First_Session').master("local[*]").getOrCreate()

# Example: Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# Stop SparkSession
spark.stop()