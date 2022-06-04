import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('year', T.IntegerType(), True),
    T.StructField('month_id', T.IntegerType(), True),
    T.StructField('day', T.IntegerType(), True),
    T.StructField('arrival_date', T.DateType(), True),
    T.StructField('airline', T.StringType(), True),
    T.StructField('flight_number', T.StringType(), True),
    T.StructField('port_id', T.StringType(), True),
    T.StructField('citizenship_id', T.IntegerType(), True),
    T.StructField('residence_id', T.IntegerType(), True),
    T.StructField('age', T.IntegerType(), True),
    T.StructField('age_id', T.IntegerType(), True),
    T.StructField('gender_id', T.StringType(), True),
    T.StructField('visa_id', T.IntegerType(), True),
    T.StructField('address_id', T.StringType(), True),
    T.StructField('stay', T.IntegerType(), True),
    T.StructField('stay_id', T.IntegerType(), True),
    T.StructField('count', T.IntegerType(), True),
])


class ImmigrationFile(FileBase):
    def __init__(self):
        super().__init__(
            "immigration",
            schema,
            self.staging,
            partitions=['year', 'month_id'],
            writable=True
        )

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='year')
        self.add_check(self.Check.no_nulls, column='month_id')
        self.add_check(self.Check.no_nulls, column='day')
        self.add_check(self.Check.no_nulls, column='arrival_date')
        self.add_check(self.Check.no_nulls, column='airline')
        self.add_check(self.Check.no_nulls, column='flight_number')
        self.add_check(self.Check.no_nulls, column='port_id')
        self.add_check(self.Check.no_nulls, column='citizenship_id')
        self.add_check(self.Check.no_nulls, column='residence_id')
        self.add_check(self.Check.no_nulls, column='count')
        # TODO resolve referential integrity issues
        #self.add_check(self.Check.referential_integrity, table=['ports'], column='port_id')
        #self.add_check(self.Check.referential_integrity, table=['country'], column=['citizenship_id', 'country_id'])
        self.add_check(self.Check.referential_integrity, table=['country'], column=['residence_id', 'country_id'])
        #self.add_check(self.Check.referential_integrity, table=['states'], column=['address_id', 'state_id'])