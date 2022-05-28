import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = (
    T.StructType([
        T.StructField('Type', T.StringType(), True),
        T.StructField('Name', T.StringType(), True),
        T.StructField('Abbreviation', T.StringType(), True),
        T.StructField('Capital', T.StringType(), True),
        T.StructField('Population (2015)', T.StringType(), True),
        T.StructField('Population (2019)', T.StringType(), True),
        T.StructField('area (square miles)', T.StringType(), True),
    ])
)

class RawStatesFile(FileBase):
    def __init__(self):
        super().__init__(
            "us-states-territories.csv",
            schema,
            self.raw,
            format='csv',
            options={
                'encoding': 'ISO-8859-1',
                'header': 'true'
            }
        )
