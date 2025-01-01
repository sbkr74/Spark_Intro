import os
import threading
import shutil
from pyspark.sql import SparkSession
import time
# Global thread lock
cleanup_lock = threading.Lock()

def safe_cleanup(dir_path):
    """Thread-safe cleanup function."""
    # with cleanup_lock:  # Acquire the lock
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        try:
            # Iterate through the contents of the directory
            for item in os.listdir(dir_path):
                item_path = os.path.join(dir_path, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)  # Remove file or symlink
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)  # Remove directory
            print(f"Contents of {dir_path} deleted successfully.")
        except Exception as e:
            print(f"Error deleting contents of {dir_path}: {e}")

def custom_shutdown_hook(temp_dir):
    """Custom shutdown hook to delete the Spark temp directory."""
    print(f"Running custom shutdown hook for {temp_dir}")
    safe_cleanup(temp_dir)

# Main Spark Application
spark = SparkSession.builder \
    .appName("ThreadLockExample") \
    .config("spark.local.dir", "C:\\spark\\temp\\") \
    .config("spark.local.dir.cleanupOnExit", "false") \
    .getOrCreate()

print("Spark Session Created")
temp_dir = spark.conf.get("spark.local.dir")

# Example operation
data = [(25, "Mumbai", "Arya"), (30, "Bhopal", "Danny")]
df = spark.createDataFrame(data, ["Age", "City", "Name"])
df.show()

# Register a shutdown hook with the lock
import atexit
atexit.register(custom_shutdown_hook, temp_dir)

# Stop Spark Session
spark.stop()

print("\nspark is being stopped\n")

# Final manual cleanup (if necessary)
safe_cleanup(temp_dir)
