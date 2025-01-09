from pyspark.sql import SparkSession
import os
import uuid
import time

# Define a unique custom temporary directory path
custom_tmp_dir = f"C:\\spark\\temp\\{uuid.uuid4()}"
os.makedirs(custom_tmp_dir, exist_ok=True)
print(f"Custom Temp Directory Created: {custom_tmp_dir}")

# Build the Spark session with the updated configuration
spark = SparkSession.builder \
    .appName("Custom Temp Dir Example") \
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={custom_tmp_dir}") \
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={custom_tmp_dir}") \
    .config("spark.local.dir", custom_tmp_dir) \
    .config("spark.cleaner.referenceTracking", "false") \
    .config("spark.cleaner.referenceTracking.blocking", "true") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .config("spark.local.dir.cleanupOnExit", "false") \
    .getOrCreate()

print("Spark Session Created Successfully")

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

time.sleep(2)
# Stop Spark
spark.stop()
print("Spark Session Stopped")

# Debugging: Check if the temp directory still exists
if os.path.exists(custom_tmp_dir):
    print(f"Temp Directory Exists After Stop: {custom_tmp_dir}")
else:
    print("Temp Directory Already Deleted During Shutdown")

