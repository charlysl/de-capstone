import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('dt', T.DateType(), True),
    T.StructField('AverageTemperature', T.DoubleType(), True),
    T.StructField('AverageTemperatureUncertainty', T.StringType(), True),
    T.StructField('State', T.StringType(), True),
    T.StructField('Country', T.StringType(), True),
])

class RawTemperaturesFile(FileBase):
    def __init__(self):
        super().__init__(
            "GlobalLandTemperaturesByState.csv",
            schema,
            self.raw,
            format='csv',
            coalesce=1,
            options={'header': 'true'}
        )