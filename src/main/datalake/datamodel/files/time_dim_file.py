import pyspark.sql.types as T

from datalake.model.dim_file_base import DimFileBase

schema = (
    T.StructType([
        T.StructField('time_id', T.StringType(), False),  # md5
        T.StructField('date', T.DateType(), False),
        T.StructField('year', T.IntegerType(), False),
        T.StructField('month_id', T.IntegerType(), False),
        T.StructField('day', T.IntegerType(), False),
        T.StructField('weekday_id', T.IntegerType(), False),
        T.StructField('month', T.StringType(), False),
        T.StructField('weekday', T.StringType(), False),
        T.StructField('weekend', T.BooleanType(), False)
    ])
)

class TimeDimFile(DimFileBase):
    def __init__(self):
        super().__init__(
            "time_dim",
            schema
        )

    nk = ['date']