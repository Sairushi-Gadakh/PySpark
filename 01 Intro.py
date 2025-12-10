from pyspark import SparkContext
sc = SparkContext("local", "WordCount")
txt = "PySpark makes big data processing fast and easy with Python"
rdd = sc.parallelize([txt]) 
# sc.parallelize() creates an RDD from the text.

counts = rdd.flatMap(lambda x: x.split()) \  
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b)
                                             # reduceByKey() adds up word counts.
# flatMap() splits lines into words
# map() creates (word, 1) pairs
# reduceByKey() adds up word counts.

print(counts.collect())
# collect() gathers the final word count output from all Spark worker nodes to the driver.

sc.stop()
# sc.stop() stops the SparkContext to free up system resources.