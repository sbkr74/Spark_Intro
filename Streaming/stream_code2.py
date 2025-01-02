from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .master("local[2]") \
    .getOrCreate()

# Set the log level
spark.sparkContext.setLogLevel("ERROR")

# Read data from socket as a structured stream
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split lines into words
from pyspark.sql.functions import explode, split
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Count occurrences of each word
word_counts = words.groupBy("word").count()

# Output the results to the console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for the streaming to finish
query.awaitTermination()
