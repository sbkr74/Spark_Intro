from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("text conversion").getOrCreate()

# Relative File path for CSV file.
file_Path = r"Docs/files/customers-100.csv"  # Use forward slashes for better cross-platform support

# Read CSV into DataFrame
cust_df = spark.read.option("header", "True").option("inferSchema", "True").csv(file_Path)

# top record
print("\nOnly showing first record from top\n")

# Get first record as a Row object
first_rec = [cust_df.first()]  # Convert Row to a list
# Convert to DataFrame
df_first = spark.createDataFrame(first_rec)
# Show Result
df_first.show()

# Top 5 records using head()
print("\nShowing Top 5 records from top Using head()\n")
head_rec = cust_df.head(5)
# Specifying schema ensures that df maintains the correct column names and data types.
df_head = spark.createDataFrame(head_rec,schema=cust_df.schema)
# Show Result 
df_head.show() 

# Top 5 records using take()
print("\nShowing Top 5 records from top Using take()\n")
take_rec = cust_df.take(5)
# Specifying schema ensures that df maintains the correct column names and data types.
df_take = spark.createDataFrame(take_rec,schema=cust_df.schema)
# Show Result
df_take.show()

# Top 5 records using show()
print("\nShowing Top 5 records from top Using show()\n")
cust_df.show(5)

# Last 10 records
print("\nShowing Last 10 records Using tail()\n")
tail_rec = cust_df.tail(10)
# Specifying schema ensures that df maintains the correct column names and data types.
df_tail = spark.createDataFrame(tail_rec,schema=cust_df.schema)
# Show Result
df_tail.show()