from pyspark.sql import SparkSession

# Define the custom temporary directory path
custom_tmp_dir = "C:\\spark\\temp"

# Build the Spark session with the custom java.io.tmpdir setting
spark = SparkSession.builder \
    .appName("Basic_Dataframe") \
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={custom_tmp_dir}") \
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={custom_tmp_dir}") \
    .config("spark.local.dir", "C:\\spark\\temp") \
    .config("spark.cleaner.referenceTracking", "false") \
    .getOrCreate()

# # Set log level to INFO
# spark.sparkContext.setLogLevel("INFO")

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