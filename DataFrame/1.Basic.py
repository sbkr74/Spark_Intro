from pyspark.sql import SparkSession

# Define data as a list of dictionaries
data = [
    {'Name': 'Arya', 'Age': 25, 'City': 'Mumbai'},
    {'Name': 'Delhi', 'Age': 30, 'City': 'Bhopal'},
    {'Name': 'Charlie', 'Age': 35, 'City': 'Chicago'},
    {'Name': 'Binita', 'Age': 25, 'City': 'Kolkata'},
    {'Name': 'Ramesh', 'Age': 30, 'City': 'Patna'},
    {'Name': 'Anjali', 'Age': 25, 'City': 'Hyderabad'},
    {'Name': 'Vishal', 'Age': 30, 'City': 'Ranchi'},
    {'Name': 'Rahul', 'Age': 25, 'City': 'Chennai'},
    {'Name': 'Vikash', 'Age': 30, 'City': 'Delhi'}
]
spark = SparkSession.builder.appName('Basic_Dataframe').getOrCreate()
df = spark.createDataFrame(data)
df.show()

# Stop Spark
spark.stop()