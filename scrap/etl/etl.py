import findspark
findspark.init()

from pyspark.sql import SparkSession

class SparkETL():
    datalake_dir = 'datalake'

    def __init__(self):
        self.spark = SparkSession.builder.appName('de-capstone').getOrCreate()

    def path(self, filename):
        return f"{self.datalake_dir}/clean/{filename}"
    
    def save_clean_table(self, df, filename, partition=None):
        """
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        """
        (
          df.write
          .format('parquet')
          .partitionBy(partition)
          .mode('OVERWRITE')
          .save(self.path(filename))
        )
    
    def read_clean_table(self, filename):
        return self.spark.read.format('parquet').load(self.path(filename))