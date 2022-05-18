"""
To import from a Jupyter notebook, execute first inside a cell:
```
import findspark
findspark.init()
```
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from datetime import datetime
import sys


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
    datalake_dir = '/Users/charly/DataEng2022/de-capstone/datalake'

    i94_data_dictionary = f"{datalake_dir}/raw/i94_data_dictionary.json"

    data_sources = {
        'i94_data_dictionary': f"{datalake_dir}/raw/I94_SAS_Labels_Descriptions.SAS",
        'immigration': f"{datalake_dir}/raw/immigration",
        'states': f"{datalake_dir}/raw/us-states-territories.csv",
        'ports': i94_data_dictionary,
        'airports': f"{datalake_dir}/raw/airport-codes_csv.csv",
        'demographics': f"{datalake_dir}/raw/us-cities-demographics.csv",
        'temperature': f"{datalake_dir}/raw/GlobalLandTemperaturesByState.csv"
    }

    def __init__(self, app_name='appname-set-by-sparksubmitoperator'):
        self.app_name = app_name
        self.spark = None

    def get_spark(self):
        if not self.spark:
            self.spark = (
                SparkSession.builder.appName('de-capstone')
                .config('spark.sql.shuffle.partitions', 5)
                .config('spark.executor.memory', '1g')
                .getOrCreate()
            )
        return self.spark

    def path(self, filename, kind='clean'):
        kinds = ('clean', 'dim', 'fact', 'test')

        if kind not in kinds:
            raise ArgumentError(f"bad kind: {kind}")

        return f"{self.datalake_dir}/{kind}/{filename}"
    
    def save_clean_table(self, df, filename, partitions=None, mode='OVERWRITE'):
        self.save_table(
            df,
            filename,
            mode,
            partitions,
            'clean'
        )
    
    def save_test_table(self, df, filename, partitions=None, mode='OVERWRITE'):
        self.save_table(
            df,
            filename,
            mode,
            partitions,
            'test'
        )

    def init_table(self, df, filename, partitions=None, kind=None):
        """
        Idempotent.
        """
        self.save_table(
            df,
            filename,
            'IGNORE',
            partitions, 
            kind
        )


    def init_dim_table(self, df, filename, partitions=None):
        self.init_table(
            df,
            filename,
            partitions, 
            kind='dim'
        )

    def init_fact_table(self, df, filename, partitions=None):
        self.init_table(
            df,
            filename,
            partitions, 
            kind='fact'
        )

    def save_dim_table(self, df, filename, partitions=None):
        self.save_table(
            df,
            filename,
            'APPEND',
            partitions, 
            'dim'
        )


    def save_fact_table(self, df, filename, partitions=None):
        self.save_table(
            df,
            filename,
            'APPEND',
            partitions, 
            'fact'
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
        return self.read_table(filename, 'clean')

    def read_dim_table(self, filename):
        return self.read_table(filename, 'dim')

    def read_fact_table(self, filename):
        return self.read_table(filename, 'fact')

    def date_to_datetime(date):
        return datetime.strptime(date, '%Y-%m-%d')

    def filter_one_month(df, date):
        """
        Filter out the rows that are not in the given year and month.
        
        Parameters:
        - df: an immigration Spark DataFrame
        - date: a string with format 'YYYY-MM-DD'
        
        Returns: a Spark DataFrame with only the rows in the given year and month.
        """

        dt = SparkETL.date_to_datetime(date)

        return (
            df
            .where(
                (F.col('year') == dt.year)
                & (F.col('month_id') == dt.month)
            )
        )
    
    def ifnull_str_expr(col, alias=None):
        return (
            F.expr(f"IF({col} IS NULL, 'UNKNOWN', {col})")
            .alias(alias if alias else col)
        )

    def ifnull_num_expr(col, alias=None):
        return (
            F.expr(f"IF({col} IS NULL, 9999, CAST({col} AS INT))")
            .alias(alias if alias else col)
        )

    def get_date():
        return sys.argv[1]