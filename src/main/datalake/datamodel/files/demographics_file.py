import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('state_id', T.StringType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('asian', T.DoubleType(), True),
    T.StructField('black', T.DoubleType(), True),
    T.StructField('latino', T.DoubleType(), True),
    T.StructField('native', T.DoubleType(), True),
    T.StructField('white', T.DoubleType(), True),
    T.StructField('ethnicity_id', T.IntegerType(), True),
    T.StructField('ethnicity', T.StringType(), True),
    T.StructField('population', T.IntegerType(), True),
    T.StructField('size_id', T.IntegerType(), True),
    T.StructField('size', T.StringType(), True)
])


class DemographicsFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "demographics",
            schema,
            self.curated,
            writable=True
        )