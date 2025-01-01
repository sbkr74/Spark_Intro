import os
import shutil
import atexit
import time
from pyspark.sql import SparkSession

def custom_cleanup(temp_dir):
    """Custom function to delete the Spark temp directory after default cleanup."""
    print(f"Running custom cleanup for {temp_dir}")
    
    # Ensure that directory exists before attempting cleanup
    if os.path.exists(temp_dir) and os.path.isdir(temp_dir):
        try:
            # Perform safe deletion of contents
            for item in os.listdir(temp_dir):
                item_path = os.path.join(temp_dir, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            print(f"Contents of {temp_dir} deleted successfully.")
        except Exception as e:
            print(f"Error while cleaning up {temp_dir}: {e}")
    else:
        print(f"{temp_dir} does not exist or was already cleaned up.")

def custom_shutdown_hook(temp_dir):
    """Custom shutdown hook, called after Spark default cleanup."""
    print("Custom shutdown hook initiated.")
    time.sleep(2)  # Simulate some work before cleanup
    custom_cleanup(temp_dir)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ShutdownHookTest") \
    .config("spark.local.dir", "C:/spark/temp") \
    .getOrCreate()

print("Spark Session Created")
df = spark.createDataFrame([(25, "Mumbai", "Arya"), (30, "Bhopal", "Danny")], ["Age", "City", "Name"])
df.show()

# Register custom shutdown hook to run after Spark's cleanup
temp_dir = spark.conf.get("spark.local.dir")
atexit.register(custom_shutdown_hook, temp_dir)

# Manually stop Spark, but delay cleanup to avoid conflict
print("\nStopping Spark session")
spark.stop()
print("\nSpark stopped. Default cleanup hooks should have executed.")

# Wait a moment before finishing the program to allow cleanup
time.sleep(5)  # Ensure the shutdown hook has time to execute
