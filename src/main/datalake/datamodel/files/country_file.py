import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('country_id', T.IntegerType(), True),
    T.StructField('country', T.StringType(), True),
])

class CountryFile(FileBase):
    def __init__(self):
        super().__init__(
            "country",
            schema,
            self.curated,
            writable=True
        )