import pyspark.sql.types as T

import os

from datalake.model.file_base import FileBase
from datalake.utils import spark_helper


def create_file(test_name,
                area=FileBase.staging, arity=1, empty=None,
                **kwargs):
    if empty:
        data = [None]
    else:
        data = [[f'{test_name}_data{i}' for i in range(arity)]]
    
    df = create_df(data, arity)
    name = f'{test_name}_file'
    schema = df.schema

    return (
        FileBase(name, schema, area, **kwargs),
        data,
        df
    )

def create_df(data, arity=None):
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
            create_schema(arity)
        )
    )

def create_schema(arity):
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

def set_s3_hdfs_roots():
    os.environ['DATALAKE_ROOT'] = 's3a://de-capstone-2022/datalake_test'
    os.environ['STAGING_ROOT'] = 'hdfs://localhost:9000/staging_test'
