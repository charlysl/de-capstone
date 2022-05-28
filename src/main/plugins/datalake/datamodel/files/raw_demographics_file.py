import pyspark.sql.types as T

from datalake.model.file_base import FileBase

schema = T.StructType([
    T.StructField('City', T.StringType(), True),
    T.StructField('State', T.StringType(), True),
    T.StructField('Median Age', T.StringType(), True),
    T.StructField('Male Population', T.StringType(), True),
    T.StructField('Female Population', T.StringType(), True),
    T.StructField('Total Population', T.IntegerType(), True),
    T.StructField('Number of Veterans', T.StringType(), True),
    T.StructField('Foreign-born', T.StringType(), True),
    T.StructField('Average Household Size', T.StringType(), True),
    T.StructField('State Code', T.StringType(), True),
    T.StructField('Race', T.StringType(), True),
    T.StructField('Count', T.StringType(), True)
])

class RawDemographicsFile(FileBase):
    def __init__(self):
        super().__init__(
            "us-cities-demographics.csv",
            schema,
            self.raw,
            format='csv',
            coalesce=1,
            options={
                'header': True,
                'sep': ';'
            }
        )