import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('country_id', T.IntegerType(), True),
    T.StructField('country', T.StringType(), True),
])

class CountryFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "country",
            schema,
            self.curated
        )