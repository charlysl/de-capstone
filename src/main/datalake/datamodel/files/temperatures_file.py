import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('state_id', T.StringType(), True),
    T.StructField('climate_id', T.IntegerType(), True),
    T.StructField('climate', T.StringType(), True),
])

class TemperaturesFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "temperatures",
            schema,
            self.curated,
            writable=True
        )

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='state_id')
        self.add_check(self.Check.no_nulls, column='climate_id')
        self.add_check(self.Check.no_nulls, column='climate')
        self.add_check(self.Check.no_duplicates, column='state_id')
