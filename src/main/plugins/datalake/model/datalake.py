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




class SparkDatalake():
    #root = '/Users/charly/DataEng2022/de-capstone/datalake-test'

    def __init__(self, root):
        self.root = root
        self.spark = None

        self.i94_data_dictionary = f"{self.root}/raw/i94_data_dictionary.json"

        self.data_sources = {
            'i94_data_dictionary': f"{self.root}/raw/I94_SAS_Labels_Descriptions.SAS",
            'immigration': f"{self.root}/raw/immigration",
            
            'ports': self.i94_data_dictionary,
            'airports': f"{self.root}/raw/airport-codes_csv.csv",
            'demographics': f"{self.root}/raw/us-cities-demographics.csv",
            'temperature': f"{self.root}/raw/GlobalLandTemperaturesByState.csv"
        }

    def get_spark(self):
        """
        appName will be ignored if executed inside a cluster (submit),
        but will be honoured when executed in a shell (findspark)
        TODO: config parameter
        """
        if not self.spark:
            self.spark = (
                SparkSession.builder.appName('de-capstone')
                .config('spark.sql.shuffle.partitions', 5)
                .config('spark.executor.memory', '1g')
                .getOrCreate()
            )
        return self.spark        

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

