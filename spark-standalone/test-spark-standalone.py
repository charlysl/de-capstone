from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName('test-spark-standalone').getOrCreate()

df = spark.createDataFrame(['It works!'], 'string')
print('[*** ', df.collect()[0][0], ' ***]')

