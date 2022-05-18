import findspark

import unittest

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as T

from etl import SparkETL


class ETLTestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        findspark.init()
        cls.spark = (
            SparkSession.builder.appName('test-etl-validation').getOrCreate()
        )
        cls.reduce_spark_logging()

    # test helpers

    @classmethod
    def create_df(cls, data):
        """
        Description: create Spark DataFrame
        Parameters:
            data :list(list(str)) - the data frame's data
        Returns: a data frame with schema (col0, ...., col{len(data[0])-1})
        """
        arity = len(data[0]) if len(data) > 0 else 1
        return (
            cls.spark.createDataFrame(
                data,
                cls._create_schema(arity)
            )
        )

    @classmethod
    def _create_schema(cls, arity):
        fields = [
            T.StructField(
                f"col{i}",
                T.StringType(),
                True)
            for i in range(arity)
        ]
        return T.StructType(fields)

    @classmethod
    def reduce_spark_logging(cls):
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def save_df(clas, df, name):
        """
        Description: save df as parquet file "name" of kind "test"
        """
        SparkETL().save_test_table(df, name)
