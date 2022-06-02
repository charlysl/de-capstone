import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('port_id', T.StringType(), True),
    T.StructField('airport_id', T.StringType(), True),
])

class PortsToAirportsFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "ports_to_airports",
            schema,
            self.curated,
            coalesce=1,
            writable=True
        )

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='port_id')
        self.add_check(self.Check.no_nulls, column='airport_id')
        self.add_check(self.Check.no_duplicates, column='port_id')
        #self.add_check(self.Check.no_duplicates, column='airport_id')
        self.add_check(self.Check.no_duplicates, column=['port_id', 'airport_id'])
        self.add_check(self.Check.referential_integrity, table='ports', column='port_id')
        self.add_check(self.Check.referential_integrity, table='airports', column='airport_id')