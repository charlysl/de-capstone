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

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='state_id')
        self.add_check(self.Check.no_nulls, column='city')
        self.add_check(self.Check.no_nulls, column='ethnicity_id')
        self.add_check(self.Check.no_nulls, column='size_id')
        self.add_check(self.Check.no_duplicates, column=['state_id', 'city'])
        self.add_check(self.Check.referential_integrity, table=['states'], column='state_id')        