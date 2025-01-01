import time
import os
from pyspark.sql import SparkSession

# Custom function to safely delete Spark temp directory
def safely_delete_temp_directory(temp_dir):
    print(f"Custom Temp Directory Exists After Stop: {temp_dir}")
    if os.path.exists(temp_dir):
        print(f"Sleeping to let auto-cleanup complete...")
        time.sleep(5)  # Delay to allow auto-cleanup to finish
        if os.path.exists(temp_dir):  # Check again after the delay
            print(f"Manually deleting temp directory: {temp_dir}")
            os.rmdir(temp_dir)  # Deletes the directory only if empty
        else:
            print(f"Temp directory already cleaned up: {temp_dir}")
    else:
        print(f"No temp directory found for cleanup.")

# Create SparkSession with custom temp directory
temp_dir = "C:\\spark\\temp"
os.makedirs(temp_dir, exist_ok=True)  # Ensure the temp directory exists

print(f"Custom Temp Directory Created/Exists: {temp_dir}")

spark = SparkSession.builder \
    .appName("TempDirectoryRaceConditionExample") \
    .config("spark.local.dir", temp_dir) \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session Created Successfully")

# Example DataFrame
data = [
    (25, "Mumbai", "Arya"),
    (30, "Bhopal", "Danny"),
    (28, "Chicago", "Charlie"),
    (23, "Kolkata", "Binita"),
    (27, "Patna", "Ramesh"),
    (25, "Hyderabad", "Anjali"),
    (30, "Ranchi", "Vishal"),
    (33, "Chennai", "Rahul"),
    (28, "Delhi", "Vikash")
]
columns = ["Age", "City", "Name"]

df = spark.createDataFrame(data, columns)
df.show()

print("Spark Configured Local Temp Directory:", temp_dir)

# Stop SparkSession
spark.stop()
print("Spark Session Stopped")

# Safely delete temp directory after Spark shutdown
safely_delete_temp_directory(temp_dir)
