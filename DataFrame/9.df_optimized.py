from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("text conversion").getOrCreate()

# Relative File path for CSV file.
file_Path = r"Docs/files/customers-100.csv"  # Use forward slashes for better cross-platform support

# Read CSV into DataFrame
cust_df = spark.read.option("header", "True").option("inferSchema", "True").csv(file_Path)

# Get first record as a Row object
first_rec = [cust_df.first()]  # Convert Row to a list

# Convert to DataFrame
df_first = spark.createDataFrame(first_rec)

# Show result
df_first.show()
