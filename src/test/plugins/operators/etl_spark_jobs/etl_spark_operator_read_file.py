from datalake.model.file_base import FileBase

from datalake.utils import spark_helper

import pyspark.sql.types as T
schema = T.StructType([T.StructField('col', T.StringType(), True)])

def create_dummy_file():
    class DummyFile(FileBase):
        def __init__(self):
            super().__init__(
                'dummy',
                schema,
                self.staging
            )
    return DummyFile()

def save_dummy_file():
    file = create_dummy_file()
    df = create_dummy_dataframe()
    file.save(df, force=True)

def read_dummy_file():
    file = create_dummy_file()
    file.read()

def create_dummy_dataframe():
    return (
        spark_helper.get_spark()
        .createDataFrame(
            [],
            schema
        )
    )

save_dummy_file()
read_dummy_file()

create_dummy_file()
read_dummy_file()