"""
To import from a Jupyter notebook, execute first inside a cell:
```
import findspark
findspark.init()
```
"""

from pyspark.sql import SparkSession, DataFrame

def pipe(self, *args):
    """
    Description: a pandas-like pipe function

    Input: 
    - self: a Spark DataFrame
    - fn: a function with signature   DataFrame: DataFrame

    Output: a Spark Dataframe
    """
    return args[0](self, *args[1:])

# TODO assert DataFrame does not have pipe method, API might add it
DataFrame.pipe = pipe


class SparkETL():
    datalake_dir = 'datalake'

    i94_data_dictionary = f"{datalake_dir}/raw/i94_data_dictionary.json"

    data_sources = {
        'states': '../raw/states/us-states-territories.csv',
        'ports': i94_data_dictionary,
        'airports': '../../airport-codes_csv.csv',
        'demographics': '../../us-cities-demographics.csv',
        'temperature': '../../GlobalLandTemperaturesByState.csv'
    }

    def __init__(self):
        self.spark = None

    def get_spark(self):
        if not self.spark:
            self.spark = SparkSession.builder.appName('de-capstone').getOrCreate()
        return self.spark

    def path(self, filename, kind='clean'):
        kinds = ('clean', 'dim', 'fact')

        if kind not in kinds:
            raise ArgumentError(f"bad kind: {kind}")

        return f"{self.datalake_dir}/{kind}/{filename}"
    
    def save_clean_table(self, df, filename, partitions=None):
        self.save_table(
            df,
            filename,
            'OVERWRITE',
            partitions,
            'clean'
        )

    def init_dim_table(self, df, filename, partitions=None):
        """
        Idempotent.
        """
        self.save_table(
            df,
            filename,
            'IGNORE',
            partitions, 
            'dim'
        )

    def save_dim_table(self, df, filename, partitions=None):
        self.save_table(
            df,
            filename,
            'APPEND',
            partitions, 
            'dim'
        )

    def save_table(self, df, filename, mode, partitions, kind):
        """
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        """
        
        df_writer = self.set_partitioning(df.write, partitions)
        path = self.path(filename, kind)

        (
          df_writer
          .format('parquet')
          .mode(mode)
          .save(path)
        )
    
    def set_partitioning(self, df_writer, partitions):
        return df_writer.partitionBy(partitions) if partitions else df_writer

    def read_table(self, filename, kind):
        return (
            self
            .get_spark()
            .read
            .format('parquet')
            .load(self.path(filename, kind))
        )

    def read_clean_table(self, filename):
        self.read_table(filename, 'clean')

    def read_dim_table(self, filename):
        self.read_table(filename, 'dim')
