import pyspark.sql.types as T

from datalake.model.file_base import FileBase

from datetime import datetime

"""
schema = T.StructType([
    T.StructField('cicid', T.FloatType(), True),
    T.StructField('i94yr', T.FloatType(), True),
    T.StructField('i94mon', T.FloatType(), True),
    T.StructField('i94cit', T.FloatType(), True),
    T.StructField('i94res', T.FloatType(), True),
    T.StructField('i94port', T.StringType(), True),
    T.StructField('arrdate', T.FloatType(), True),
    T.StructField('i94mode', T.FloatType(), True),
    T.StructField('i94addr', T.StringType(), True),
    T.StructField('depdate', T.FloatType(), True),
    T.StructField('i94bir', T.FloatType(), True),
    T.StructField('i94visa', T.FloatType(), True),
    T.StructField('count', T.FloatType(), True),
    T.StructField('dtadfile', T.StringType(), True),
    T.StructField('visapost', T.StringType(), True),
    T.StructField('occup', T.StringType(), True),
    T.StructField('entdepa', T.StringType(), True),
    T.StructField('entdepd', T.StringType(), True),
    T.StructField('entdepu', T.StringType(), True),
    T.StructField('matflag', T.StringType(), True),
    T.StructField('biryear', T.FloatType(), True),
    T.StructField('dtaddto', T.StringType(), True),
    T.StructField('gender', T.StringType(), True),
    T.StructField('insnum', T.StringType(), True),
    T.StructField('airline', T.StringType(), True),
    T.StructField('admnum', T.FloatType(), True),
    T.StructField('fltno', T.StringType(), True),
    T.StructField('visatype', T.StringType(), True),
    # only in 2016-06-01:
    T.StructField('validres', T.DoubleType(), True),
    T.StructField('delete_days', T.DoubleType(), True),
    T.StructField('delete_mexl', T.DoubleType(), True),
    T.StructField('delete_dup', T.DoubleType(), True),
    T.StructField('delete_visa', T.DoubleType(), True),
    T.StructField('delete_recdup', T.DoubleType(), True),
])
"""

class SasFile(FileBase):
    def __init__(self, date):
        super().__init__(
            self.sas_file_path(date),
            None, # schema is different for 2016-06
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