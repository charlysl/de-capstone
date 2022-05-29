import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('state_id', T.StringType(), True),
    T.StructField('climate_id', T.IntegerType(), True),
    T.StructField('climate', T.StringType(), True),
])

class TemperaturesFile(FileBase):
    def __init__(self):
        super().__init__(
            "temperatures",
            schema,
            self.curated,
            writable=True
        )