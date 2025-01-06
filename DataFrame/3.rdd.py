from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('rdd example').setMaster('local')
sc = SparkContext(conf=conf)
num_list = [3,2,1,4,5,6,9,9,7,8]
print(type(sc))
rdd = sc.parallelize(num_list)
print(type(rdd))
result = rdd.collect()
print(result)

####################################
# example: 2
str_list = ["my","home","is","far","from","noises."]
rdd2 = sc.parallelize(str_list)
print(rdd2.collect())

# use of take(n)
print(rdd.take(3))

#using foreach method
rdd2.foreach(lambda x:print(x))