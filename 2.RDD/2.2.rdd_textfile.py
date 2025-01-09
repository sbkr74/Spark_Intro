from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('rdd_text').setMaster('local')
sc = SparkContext(conf=conf)
file_path = r'Docs\files\test.txt'
rdd = sc.textFile(file_path)
result = rdd.collect()
for ele in result:
    print(ele)

######################
rdd2 = rdd.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda x : (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
print(rdd4.collect())