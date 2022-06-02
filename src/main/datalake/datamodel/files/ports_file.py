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

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='port_id')
        self.add_check(self.Check.no_nulls, column='state_id')
        self.add_check(self.Check.no_nulls, column='name')
        self.add_check(self.Check.no_duplicates, column='port_id')
        #self.add_check(self.Check.no_duplicates, column='name')
        self.add_check(self.Check.referential_integrity, table=['states'], column='state_id')

