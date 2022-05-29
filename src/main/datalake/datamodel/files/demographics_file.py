import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('state_id', T.IntegerType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('asian', T.FloatType(), True),
    T.StructField('black', T.FloatType(), True),
    T.StructField('latino', T.FloatType(), True),
    T.StructField('native', T.FloatType(), True),
    T.StructField('white', T.FloatType(), True),
    T.StructField('ethnicity_id', T.IntegerType(), True),
    T.StructField('ethnicity', T.StringType(), True),
    T.StructField('population', T.IntegerType(), True),
    T.StructField('size_id', T.IntegerType(), True),
    T.StructField('size', T.StringType(), True)
])


class DemographicsFile(FileBase):
    def __init__(self):
        super().__init__(
            "demographics",
            schema,
            self.curated,
            writable=True
        )