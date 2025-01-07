# Apache Spark RDD Transformations

This document provides a detailed explanation and examples of commonly used RDD transformations in Apache Spark. Transformations are lazy operations that return a new RDD by applying a function to the existing RDD.

---

## **Basic Transformations**

### 1. **`map(func)`**
Applies a given function to each element of the RDD and returns a new RDD.

**Example:**
```python
rdd = sc.parallelize([1, 2, 3, 4])
result = rdd.map(lambda x: x * 2)
print(result.collect())  # Output: [2, 4, 6, 8]
```

---

### 2. **`flatMap(func)`**
Similar to `map`, but the function can return multiple elements for each input element. The results are flattened into a single RDD.

**Example:**
```python
rdd = sc.parallelize(["hello world", "apache spark"])
result = rdd.flatMap(lambda line: line.split(" "))
print(result.collect())  # Output: ['hello', 'world', 'apache', 'spark']
```

---

### 3. **`filter(func)`**
Returns a new RDD containing only elements that satisfy the given predicate.

**Example:**
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.filter(lambda x: x % 2 == 0)
print(result.collect())  # Output: [2, 4]
```

---

### 4. **`distinct()`**
Removes duplicate elements from the RDD.

**Example:**
```python
rdd = sc.parallelize([1, 2, 2, 3, 4, 4, 5])
result = rdd.distinct()
print(result.collect())  # Output: [1, 2, 3, 4, 5]
```

---

### 5. **`union(otherRDD)`**
Returns a new RDD that contains the union of elements from both RDDs.

**Example:**
```python
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([4, 5, 6])
result = rdd1.union(rdd2)
print(result.collect())  # Output: [1, 2, 3, 4, 5, 6]
```

---

### 6. **`intersection(otherRDD)`**
Returns an RDD with elements common to both RDDs.

**Example:**
```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])
result = rdd1.intersection(rdd2)
print(result.collect())  # Output: [3, 4]
```

---

### 7. **`subtract(otherRDD)`**
Returns an RDD with elements in the first RDD that are not in the second RDD.

**Example:**
```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4])
result = rdd1.subtract(rdd2)
print(result.collect())  # Output: [1, 2]
```

---

### 8. **`cartesian(otherRDD)`**
Returns the Cartesian product of two RDDs.

**Example:**
```python
rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize(["a", "b"])
result = rdd1.cartesian(rdd2)
print(result.collect())  # Output: [(1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')]
```

---

## **Key-Value Transformations**

### 1. **`reduceByKey(func)`**
Combines values with the same key using a specified associative function.

**Example:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 2)])
result = rdd.reduceByKey(lambda x, y: x + y)
print(result.collect())  # Output: [('a', 3), ('b', 2)]
```

---

### 2. **`groupByKey()`**
Groups values by key.

**Example:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 2)])
result = rdd.groupByKey().mapValues(list)
print(result.collect())  # Output: [('a', [1, 2]), ('b', [2])]
```

---

### 3. **`mapValues(func)`**
Applies a function to each value of a key-value pair without changing the keys.

**Example:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2)])
result = rdd.mapValues(lambda x: x * 2)
print(result.collect())  # Output: [('a', 2), ('b', 4)]
```

---

### 4. **`flatMapValues(func)`**
Applies a function to values of a key-value pair, flattening the result.

**Example:**
```python
rdd = sc.parallelize([("a", "1,2"), ("b", "3,4")])
result = rdd.flatMapValues(lambda x: x.split(","))
print(result.collect())  # Output: [('a', '1'), ('a', '2'), ('b', '3'), ('b', '4')]
```

---

### 5. **`keys()`**
Extracts the keys from a key-value RDD.

**Example:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2)])
result = rdd.keys()
print(result.collect())  # Output: ['a', 'b']
```

---

### 6. **`values()`**
Extracts the values from a key-value RDD.

**Example:**
```python
rdd = sc.parallelize([("a", 1), ("b", 2)])
result = rdd.values()
print(result.collect())  # Output: [1, 2]
```

---

### 7. **`join(otherRDD)`**
Performs an inner join on two RDDs by key.

**Example:**
```python
rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", 3), ("b", 4)])
result = rdd1.join(rdd2)
print(result.collect())  # Output: [('a', (1, 3)), ('b', (2, 4))]
```

---

### 8. **`leftOuterJoin(otherRDD)`**
Performs a left outer join on two RDDs by key.

**Example:**
```python
rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", 3)])
result = rdd1.leftOuterJoin(rdd2)
print(result.collect())  # Output: [('a', (1, 3)), ('b', (2, None))]
```

---

### 9. **`rightOuterJoin(otherRDD)`**
Performs a right outer join on two RDDs by key.

**Example:**
```python
rdd1 = sc.parallelize([("a", 1)])
rdd2 = sc.parallelize([("a", 3), ("b", 4)])
result = rdd1.rightOuterJoin(rdd2)
print(result.collect())  # Output: [('a', (1, 3)), ('b', (None, 4))]
```

---

### 10. **`cogroup(otherRDD)`**
Groups data from two RDDs sharing the same key.

**Example:**
```python
rdd1 = sc.parallelize([("a", 1), ("b", 2),("c",3)])
rdd2 = sc.parallelize([("a",9),("b",8),("d",5)])
result = rdd1.cogroup(rdd2)
print(result.collect())
```

