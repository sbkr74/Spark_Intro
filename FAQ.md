# Frequently Asked Question (Warn, Error & Exception Related)
### ERROR
`ERROR` `ShutdownHookManager:` Exception while deleting Spark temp dir: C:\Users\xxxx\AppData\Local\Temp\spark-2ed906e7-11c8-4c54-914f-66f319918e50\pyspark-30a73e65-53d3-4fd1-b454-d5b318738aae
`java.nio.file.NoSuchFileException:` 

#### solution:
```
.config("spark.local.dir.cleanupOnExit", "false")
```

### WARN
`WARN:` `spark.eventLog.gcMetrics.youngGenerationGarbageCollectors`

`WARN:` `spark.eventLog.gcMetrics.youngGenerationGarbageCollectors`