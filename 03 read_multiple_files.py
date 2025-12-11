from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

path = ['/datasets/authors.csv',
        '/datasets/book_author.csv']
# To read multiple CSV files, we will pass a python list of paths of the CSV files as string type. 

files = spark.read.csv(path, sep=',',
                       inferSchema=True, header=True)

df1 = files.toPandas()
display(df1.head())
display(df1.tail())


# Read All CSV Files in Directory
# To read all CSV files in the directory, we will use * for considering each file in the directory.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    'Read All CSV Files in Directory').getOrCreate()

file2 = spark.read.csv('/content/*.csv', sep=',', 
                    inferSchema=True, header=True)

df1 = file2.toPandas()
display(df1.head())
display(df1.tail())