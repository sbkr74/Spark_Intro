# Spark_Intro
Learning Spark Basics Concepts

## Setting Up Spark Environment
### Prerequisite  
- Always try to Use `Java 11` or `Java 8` with Spark Versions.  
- `Spark 3.5.*`
- `Python 3.*`

`NOTE:` `*` states any version number.

#### Check Java Version (Java11)
```cmd
C:\User\Admin> java --version
java 11.0.25 2024-10-15 LTS
Java(TM) SE Runtime Environment 18.9 (build 11.0.25+9-LTS-256)
Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.25+9-LTS-256, mixed mode)
```

#### Check Python Version (Python 3.11.*)
```cmd
C:\User\Admin>python --version
Python 3.11.8
```

#### Check Pyspark Version (Spark 3.5.*) 
```cmd
C:\User\Admin> pyspark
Python 3.11.8 (tags/v3.11.8:db85d51, Feb  6 2024, 22:03:32) [MSC v.1937 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.1
      /_/

Using Python version 3.11.8 (tags/v3.11.8:db85d51, Feb  6 2024 22:03:32)
Spark context Web UI available at http://MSI:4040
Spark context available as 'sc' (master = local[*], app id = local-1735482592444).
SparkSession available as 'spark'.
```