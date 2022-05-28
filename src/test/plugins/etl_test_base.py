import findspark
findspark.init()
from datalake.utils import spark_helper

import unittest
import shutil

import os
os.environ['DATALAKE_ROOT'] = '/tmp/datalake'

import pyspark.sql.types as T

class ETLTestBase(unittest.TestCase):
    
    def tearDown(self):
        # It is by design that I don't use a variable to refer to the datalake
        # directory, because it could be accidentaly overwritten, risking
        # deletion of valuable files.
        if shutil.os.path.exists('/tmp/datalake'):
            shutil.rmtree('/tmp/datalake')

    # test helpers

    @classmethod
    def create_df(cls, data, arity=None):
        """
        Description: create Spark DataFrame
        Parameters:
            data :list(list(str)) - the data frame's data
        Returns: a data frame with schema (col0, ...., col{len(data[0])-1})
        """
        arity = arity if arity else len(data[0]) if len(data) > 0 else 1
        return (
            spark_helper.get_spark().createDataFrame(
                data,
                cls._create_schema(arity)
            )
        )

    @classmethod
    def _create_schema(cls, arity):
        """
        Description: create a schema of given arity and string columns
        Parameters:
        - arity: (int) number of columns
        Returns: a pyspark.sql.types.StructType
        """
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
    def save_df(cls, df, filename):
        """
        Description: save df as parquet file "name" of kind "test"
        """
        df.write.mode('overwrite').save(f"{os.environ['DATALAKE_ROOT']}/{filename}")
