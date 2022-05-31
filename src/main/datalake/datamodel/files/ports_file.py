import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('port_id', T.StringType(), True),
    T.StructField('state_id', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
])

class PortsFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "ports",
            schema,
            self.curated,
            writable=True,
            coalesce=1
        )