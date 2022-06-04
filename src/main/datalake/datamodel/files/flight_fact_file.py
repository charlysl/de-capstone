import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('time_id', T.StringType(), False),
    T.StructField('route_id', T.StringType(), False),
    T.StructField('visitor_id', T.StringType(), False),
    T.StructField('num_visitors', T.LongType(), False),
    T.StructField('age_avg', T.DoubleType(), True),
    T.StructField('age_std', T.DoubleType(), True),
    T.StructField('stay_avg', T.DoubleType(), True),
    T.StructField('stay_std', T.DoubleType(), True),
])

class FlightFactFile(FileBase):
    def __init__(self):
        super().__init__(
            "flight_fact",
            schema,
            self.production,
            writable=True,
            partitions=['year', 'month_id']
        )

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='time_id')
        self.add_check(self.Check.no_nulls, column='route_id')
        self.add_check(self.Check.no_nulls, column='visitor_id')
        self.add_check(self.Check.no_duplicates, column=['time_id', 'route_id', 'visitor_id'])
        self.add_check(self.Check.referential_integrity, table='TimeDim', column='time_id')
        self.add_check(self.Check.referential_integrity, table='RouteDim', column='route_id')
        self.add_check(self.Check.referential_integrity, table='VisitorDim', column='visitor_id')                