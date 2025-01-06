from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('rdd_text').setMaster('local')
sc = SparkContext(conf=conf)
file_path = r'Docs\files\test.txt'
rdd = sc.textFile(file_path)
result = rdd.collect()
print(result)
