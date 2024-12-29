from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Basic_Dataframe').getOrCreate()

# Define data as a list of dictionaries
data = [
    {'Name': 'Arya', 'Age': 25, 'City': 'Mumbai'},
    {'Name': 'Danny', 'Age': 30, 'City': 'Bhopal'},
    {'Name': 'Charlie', 'Age': 28, 'City': 'Chicago'},
    {'Name': 'Binita', 'Age': 23, 'City': 'Kolkata'},
    {'Name': 'Ramesh', 'Age': 27, 'City': 'Patna'},
    {'Name': 'Anjali', 'Age': 25, 'City': 'Hyderabad'},
    {'Name': 'Vishal', 'Age': 30, 'City': 'Ranchi'},
    {'Name': 'Rahul', 'Age': 33, 'City': 'Chennai'},
    {'Name': 'Vikash', 'Age': 28, 'City': 'Delhi'}
]
df = spark.createDataFrame(data)
df.show()

# Stop Spark
spark.stop()