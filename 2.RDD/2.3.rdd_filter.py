from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "filterExample")

file_path = r"Docs\files\word_count.txt"
rdd = sc.textFile(file_path)
rdd_split = rdd.flatMap(lambda x : x.split(" "))
rdd_map = rdd_split.map(lambda x : (x,1))
rdd_reduce = rdd_map.reduceByKey(lambda x,y : x+y)

# filter on values 
rdd_filter_val1 = rdd_reduce.filter(lambda x : x[1]>=5)
rdd_filter_val2 = rdd_reduce.filter(lambda x : x[1]%2 == 0)

print(rdd_filter_val1.collect(),"\n")
print(rdd_filter_val2.collect(),"\n")

# filter on keys
rdd_filter_key1 = rdd_reduce.filter(lambda x : x[0].startswith('M'))
rdd_filter_key2 = rdd_reduce.filter(lambda x : x[0].endswith('s'))
rdd_filter_key3 = rdd_reduce.filter(lambda x : "ss" in x[0])
rdd_filter_key4 = rdd_reduce.filter(lambda x : x[0].find('p')!=-1)

print(rdd_filter_key1.collect(),"\n")
print(rdd_filter_key2.collect(),"\n")
print(rdd_filter_key3.collect(),"\n")
print(rdd_filter_key4.collect(),"\n")