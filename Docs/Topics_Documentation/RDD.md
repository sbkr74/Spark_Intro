# RDD
Resilient Distributed Datasets (RDD) are a fundamental data structure in Apache Spark. They represent an immutable, distributed collection of objects that can be processed in parallel across a cluster. RDDs are designed to be fault-tolerant, meaning they can recover from node failures automatically.

At a high level, every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster. The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel.

There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.



### Summary
- RDD stands for Resilient Distributed Dataset.
    - Resilient:- Relates to fault-tolerant i.e., ability to recover from failure.
    - Distributed:- partitioned across nodes.
    - Dataset:- Collection of records which is stored in any files like csv, json text file, etc.
- It is Basic Data Structure in Spark.
- Using SparkContext we can create RDD.
- RDD is immutable.
- RDD is partitioned across worker nodes.
### There are broadly 2 types of operations.
- Transformations
- Actions


# Operations with RDD
### Transformations Operations
- distinct()
- keys()
- values()
- map(func)
- mapPartitions(func)
- mapPartitionsWithIndex(func)
- filter(func)
- flatmap(func)
- reduceByKey(func)
- groupBykey()
- mapValues(func)
- flatMapValues(func)
- join(otherRDD)
- leftOuterJoin(otherRDD)
- rightOuterJoin(otherRDD)
- union(otherRDD)
- intersection(otherRDD)
- substract(otherRDD)
- cartesian(otherRDD)

### Advance Transformations
- cogroup(otherRDD)
- coalesce(numPartitions)
- repartition(numPartitions)
- zip(otherRDD)
- sample(withReplacement,fraction,seed) -> rdd.sample(false,0.1)
- sortBy(func,ascending=True)
- aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
- repartitionAndSortWithinPartitions(partitioner)
- pipe(command)  -> external shell command -> rdd.pipe("grep 'error'")


### Actions Operations
- collect()
- reduce(func)
- count()
- countByKey()
- first()
- foreach(func)
- take(n)
- takeSample(withReplacement, num, [seed])
- takeOrdered(n, [ordering])
- saveAsTextFile(path)
- saveAsSequenceFile(path) `(Java and Scala)`
- saveAsObjectFile(path) `(Java and Scala)`