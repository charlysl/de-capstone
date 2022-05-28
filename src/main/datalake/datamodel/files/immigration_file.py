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
    T.StructField('residence_id', T.StringType(), True),
    T.StructField('age', T.StringType(), True),
    T.StructField('age_id', T.IntegerType(), True),
    T.StructField('gender_id', T.StringType(), True),
    T.StructField('visa_id', T.StringType(), True),
    T.StructField('address_id', T.StringType(), True),
    T.StructField('stay', T.StringType(), True),
    T.StructField('stay_id', T.IntegerType(), True),
    T.StructField('count', T.IntegerType(), True),
])


class ImmigrationFile(FileBase):
    def __init__(self):
        super().__init__(
            "immigration",
            schema,
            self.curated,
            partitions=['year', 'month_id']
        )