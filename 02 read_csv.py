from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    'Read CSV File into DataFrame').getOrCreate()

authors = spark.read.csv('./datasets/authors.csv', sep=',',
                         inferSchema=True, header=True)

# we passed the delimiter used in the CSV file
# we set the inferSchema attribute as True, this will go through the CSV file 
# and automatically adapt its schema into PySpark Dataframe. 

df = authors.toPandas()
# Then, we converted the PySpark Dataframe to Pandas Dataframe df using toPandas() method.

df.head()