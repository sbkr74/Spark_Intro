import os
import shutil
import atexit
import time
from pyspark.sql import SparkSession
import logging

# Set up logging to track shutdown hooks
logging.basicConfig(level=logging.DEBUG)

def custom_cleanup(temp_dir):
    """Custom function to delete the Spark temp directory after default cleanup."""
    logging.debug(f"Running custom cleanup for {temp_dir}")
    
    if os.path.exists(temp_dir) and os.path.isdir(temp_dir):
        try:
            for item in os.listdir(temp_dir):
                item_path = os.path.join(temp_dir, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            logging.debug(f"Contents of {temp_dir} deleted successfully.")
        except Exception as e:
            logging.error(f"Error while cleaning up {temp_dir}: {e}")
    else:
        logging.debug(f"{temp_dir} does not exist or was already cleaned up.")

def custom_shutdown_hook(temp_dir):
    """Custom shutdown hook, called after Spark default cleanup."""
    logging.debug("Custom shutdown hook initiated.")
    time.sleep(2)  # Simulate some work before cleanup
    custom_cleanup(temp_dir)

def monitor_shutdown():
    """Function to monitor shutdown process completion."""
    logging.debug("Monitoring ShutdownHookManager completion.")
    time.sleep(3)  # Wait for the shutdown hooks to run
    logging.debug("ShutdownHookManager monitoring complete.")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ShutdownHookTest") \
    .config("spark.local.dir", "C:/spark/temp") \
    .getOrCreate()

logging.debug("Spark Session Created")

# Sample DataFrame creation
df = spark.createDataFrame([(25, "Mumbai", "Arya"), (30, "Bhopal", "Danny")], ["Age", "City", "Name"])
df.show()

# Register custom shutdown hook to run after Spark's cleanup
temp_dir = spark.conf.get("spark.local.dir")
atexit.register(custom_shutdown_hook, temp_dir)

# Monitoring the shutdown process in the background
monitor_shutdown()

# Manually stop Spark
logging.debug("\nStopping Spark session")
spark.stop()
logging.debug("\nSpark stopped. Default cleanup hooks should have executed.")

# Wait for cleanup to finish
time.sleep(5)  # Give enough time for custom shutdown hook to finish
