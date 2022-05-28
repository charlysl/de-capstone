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
            writable=True
        )