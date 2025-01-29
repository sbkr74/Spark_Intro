from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('filter operation').getOrCreate()

file_path = r"Docs\files\person_data_quality.csv"
df = spark.read.option("Header","True").option("inferSchema","True").csv(file_path)

# for string type
filter_df = df.filter("Name != 'NULL'")
filter_df.show()

# for integer type
filter_df = df.filter("Age == 50")
filter_df.show()

# Using Col function
from pyspark.sql.functions import col
filter_df = df.filter(col("Phone") != 'NULL')
filter_df.show()

# for mutiple comdition
filter_df = df.filter("Age = 50 or Age = 30")
filter_df.show()

filter_df = df.filter((col("Age") == 50) | (col("Age") == 30))
filter_df.show()

filter_df = df.filter("Age = 50 AND Name IS NULL")
filter_df.show()

filter_df = df.filter((col("Age") == 50) & (col("Name").isNull()))
filter_df.show()

# Using col for different operations
filter_df = df.filter(col("Age").between(30,50))
filter_df.show()
