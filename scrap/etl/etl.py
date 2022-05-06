"""
To import from a Jupyter notebook, execute first inside a cell:
```
import findspark
findspark.init()
```
"""

from pyspark.sql import SparkSession, DataFrame

def pipe(self, fn):
    """
    Description: a pandas-like pipe function

    Input: 
    - self: a Spark DataFrame
    - fn: a function with signature   DataFrame: DataFrame

    Output: a Spark Dataframe
    """
    return fn(self)

# TODO assert DataFrame does not have pipe method, API might add it
DataFrame.pipe = pipe


class SparkETL():
    datalake_dir = 'datalake'

    i94_data_dictionary = f"{datalake_dir}/raw/i94_data_dictionary.json"

    data_sources = {
        'states': '../raw/states/us-states-territories.csv',
        'ports': i94_data_dictionary
    }

    def __init__(self):
        self.spark = None

    def get_spark(self):
        if not self.spark:
            self.spark = SparkSession.builder.appName('de-capstone').getOrCreate()
        return self.spark

    def path(self, filename):
        return f"{self.datalake_dir}/clean/{filename}"
    
    def save_clean_table(self, df, filename, partitions=None):
        """
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        """
        df_writer = self.set_partitioning(df.write, partitions)
        (
          df_writer
          .format('parquet')
          .mode('OVERWRITE')
          .save(self.path(filename))
        )
    
    def set_partitioning(self, df_writer, partitions):
        return df_writer.partitionBy(partitions) if partitions else df_writer


    def read_clean_table(self, filename):
        return self.get_spark().read.format('parquet').load(self.path(filename))

