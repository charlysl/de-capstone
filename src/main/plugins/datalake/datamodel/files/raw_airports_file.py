import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('ident', T.StringType(), True),
    T.StructField('type', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('elevation_ft', T.StringType(), True),
    T.StructField('continent', T.StringType(), True),
    T.StructField('iso_country', T.StringType(), True),
    T.StructField('iso_region', T.StringType(), True),
    T.StructField('municipality', T.StringType(), True),
    T.StructField('gps_code', T.StringType(), True),
    T.StructField('iata_code', T.StringType(), True),
    T.StructField('local_code', T.StringType(), True),
    T.StructField('coordinates', T.StringType(), True)
])

class RawAirportsFile(FileBase):
    def __init__(self):
        super().__init__(
            "airports.csv",
            schema,
            self.raw,
            format='csv',
            options={
                'header': 'true'
            }
        )