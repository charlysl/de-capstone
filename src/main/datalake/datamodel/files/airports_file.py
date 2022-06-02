import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('airport_id', T.StringType(), True),
    T.StructField('airport_iata', T.StringType(), True),
    T.StructField('state_id', T.StringType(), True),
    T.StructField('city', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('international', T.BooleanType(), True),
    T.StructField('type_id', T.IntegerType(), True),
    T.StructField('type', T.StringType(), True),
    T.StructField('coordinates', T.StringType(), True)
])

class AirportsFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "airports",
            schema,
            self.curated,
            writable=True
        )

        # add validation checks
        self.add_check(self.Check.not_empty),
        self.add_check(self.Check.no_nulls, column='airport_id'),
        self.add_check(self.Check.no_nulls, column='airport_iata'),
        self.add_check(self.Check.no_nulls, column='state_id'),
        self.add_check(self.Check.no_nulls, column='city'),
        self.add_check(self.Check.no_nulls, column='name'),
        self.add_check(self.Check.no_nulls, column='international'),
        self.add_check(self.Check.no_nulls, column='type_id'),
        self.add_check(self.Check.no_nulls, column='type'),
        self.add_check(self.Check.no_nulls, column='coordinates'),
        self.add_check(self.Check.no_duplicates, column='airport_id')
        self.add_check(self.Check.no_duplicates, column='airport_iata')
        self.add_check(
            self.Check.referential_integrity,
            table=['states'],
            column='state_id'
        )
