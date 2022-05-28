import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('airport_id', T.StringType(), True),
    T.StructField('airport_iata', T.StringType(), True),
    T.StructField('state_id', T.StringType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('international', T.BooleanType(), True),
    T.StructField('type_id', T.IntegerType(), True),
    T.StructField('type', T.StringType(), True),
    T.StructField('coordinates', T.StringType(), True)
])

class AirportsFile(FileBase):
    def __init__(self):
        super().__init__(
            "airports",
            schema,
            self.curated,
            writable=True
        )