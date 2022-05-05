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

    data_sources = {
        'states': '../raw/states/us-states-territories.csv'
    }

    def __init__(self):
        self.spark = SparkSession.builder.appName('de-capstone').getOrCreate()

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
        return self.spark.read.format('parquet').load(self.path(filename))

