import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('state_id', T.IntegerType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('type_id', T.IntegerType(), True),
    T.StructField('type', T.StringType(), True),
])

class StatesFile(FileBase):
    def __init__(self):
        super().__init__(
            "states",
            schema,
            self.curated,
            writable=True
        )
