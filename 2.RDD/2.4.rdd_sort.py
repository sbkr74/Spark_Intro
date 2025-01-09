from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName('rdd sort').setMaster("local")
sc = SparkContext(conf=conf)
file_path = r'Docs\files\test.txt'
rdd = sc.textFile(file_path)
rdd2 = rdd.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda x : (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

# sort by keys (asc)
rdd5 = rdd4.sortByKey()
print(rdd5.collect())

# sort by keys (desc)
rdd6 = rdd4.sortByKey(False)
print(rdd6.collect())

# sort by values (asc)
rdd7 = rdd4.sortBy(lambda x: x[1])
print(rdd7.collect())

# sort by values (desc)
rdd8 = rdd4.sortBy(lambda x: x[1],False)
print(rdd8.collect())

# show only first value -> tuple
rdd9 = rdd8.first()
print(rdd9)
print(rdd9[0],rdd9[1])

# show top 3 records -> list
rdd10 = rdd7.take(3)
print(rdd10)

# Extras 
rddKeys = rdd4.keys()
print(rddKeys.collect())

rddVals = rdd4.values()
print(rddVals.collect())