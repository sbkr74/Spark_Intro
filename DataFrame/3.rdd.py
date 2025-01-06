from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('rdd example').setMaster('local')
sc = SparkContext(conf=conf)
num_list = [3,2,1,4,5,6,9.9,7,8]
print(type(sc))
rdd = sc.parallelize(num_list)
print(type(rdd))
rdd.collect()