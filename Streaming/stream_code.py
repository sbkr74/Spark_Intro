from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Create a Spark session
spark = SparkSession.builder \
    .appName("SparkStreamingExample") \
    .master("local[2]") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Create StreamingContext with a 5-second batch interval
ssc = StreamingContext(spark.sparkContext, 5)

# Define socket text stream
lines = ssc.socketTextStream("localhost", 9999)

# Processing logic
words = lines.flatMap(lambda line: line.split(" "))
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Print output
word_counts.pprint()

# Start the computation
ssc.start()
ssc.awaitTermination()
