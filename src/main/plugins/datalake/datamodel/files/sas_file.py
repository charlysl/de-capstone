import pyspark.sql.types as T

from datalake.model.file_base import FileBase

from datetime import datetime

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

class SasFile(FileBase):
    def __init__(self, date):
        super().__init__(
            self.sas_file_path(date),
            schema,
            self.raw,
            format='com.github.saurfang.sas.spark'
        )

    def parse_date(self, date):
        return datetime.strptime(date, '%Y-%m-%d')

    def sas_file_path(self, date):
        """
        Example: 'i94_jan16_sub.sas7bdat'
        """
        dt = self.parse_date(date)
        year = datetime.strftime(dt, '%y')
        month = datetime.strftime(dt, '%b').lower()
        file = f"i94_{month}{year}_sub.sas7bdat"
        return f"18-83510-I94-Data-2016/{file}"