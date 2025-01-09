from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName('ContextExample').setMaster("local")
sc = SparkContext(conf=conf)

# Creating RDD from list of numbers
numbers = [1,2,3,7,9,5,4]
rdd = sc.parallelize(numbers)

sum_of_nums = rdd.reduce(lambda a,b: a+b)
print(f"Total: {sum_of_nums}")

# Stop the SparkContext
sc.stop()