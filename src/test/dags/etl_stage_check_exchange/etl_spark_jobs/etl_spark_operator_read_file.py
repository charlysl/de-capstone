from datalake.model.file_base import FileBase

from datalake.utils import spark_helper

import pyspark.sql.types as T
schema = T.StructType([T.StructField('col', T.StringType(), True)])

def save_dummy_file(area):
    file = create_dummy_file()
    df = create_dummy_dataframe()
    file.save(df, area=area, force=True)

def read_dummy_file(area):
    file = create_dummy_file()
    file.read(area=area)

def create_dummy_file():
    class DummyFile(FileBase):
        def __init__(self):
            super().__init__(
                'test_etl_spark_operator_read_file',
                schema,
                area=FileBase.staging
            )
    return DummyFile()

def create_dummy_dataframe():
    return (
        spark_helper.get_spark()
        .createDataFrame(
            [],
            schema
        )
    )

for area in ['staging', 'production']:
    save_dummy_file(area=area)
    read_dummy_file(area=area)
