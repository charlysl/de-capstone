import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('port_id', T.StringType(), True),
    T.StructField('state_id', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
])

class PortsFile(FileBase):
    def __init__(self):
        super().__init__(
            "ports",
            schema,
            self.curated,
            writable=True,
            coalesce=1
        )