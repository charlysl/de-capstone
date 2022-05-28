import pyspark.sql.types as T

from datalake.model.dim_file_base import DimFileBase

schema = (
    T.StructType([
        T.StructField('visitor_id', T.StringType(), False), # md5
        T.StructField('citizenship_id', T.IntegerType(), False),
        T.StructField('residence_id', T.IntegerType(), False),
        T.StructField('age_id', T.IntegerType(), False),
        T.StructField('gender_id', T.StringType(), False),
        T.StructField('visa_id', T.IntegerType(), False),
        T.StructField('stay_id', T.IntegerType(), False),
        T.StructField('address_id', T.StringType(), False),
        T.StructField('address_climate_id', T.IntegerType(), False),
        T.StructField('citizenship', T.StringType(), False),
        T.StructField('residence', T.StringType(), False),
        T.StructField('age', T.StringType(), False),
        T.StructField('gender', T.StringType(), False),
        T.StructField('visa', T.StringType(), False),
        T.StructField('stay', T.StringType(), False),
        T.StructField('address', T.StringType(), False),
        T.StructField('address_climate', T.StringType(), False),
    ])
)

class VisitorDimFile(DimFileBase):
    def __init__(self):
        super().__init__(
            "visitor_dim",
            schema,
        )

    nk = ['citizenship_id', 'residence_id', 'age_id', 'gender_id', 'visa_id', 'address_id', 'stay_id']
