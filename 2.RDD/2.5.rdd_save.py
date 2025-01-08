from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
# Initialize SparkContext
sc = SparkContext("local", "filterExample")
spark =SparkSession.builder.getOrCreate()
origin_file_path = r"Docs\files\word_count.txt"
dest_file_path = r"Docs\files\output"

# rdd operations
rdd = sc.textFile(origin_file_path)
rdd_split = rdd.flatMap(lambda x : x.split(" "))
rdd_map = rdd_split.map(lambda x : (x,1))
rdd_reduce = rdd_map.reduceByKey(lambda x,y : x+y)
rdd_filter = rdd_reduce.filter(lambda x : "ss" in x[0])

# save as text file
rdd_filter.saveAsTextFile(dest_file_path)

time.sleep(2)
# Read text file and display it 
text_file = spark.read.text(dest_file_path)
text_file.show(truncate = False)