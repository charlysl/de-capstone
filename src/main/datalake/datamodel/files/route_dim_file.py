import pyspark.sql.types as T

from datalake.model.dim_file_base import DimFileBase

schema = (
    T.StructType([
        T.StructField('route_id', T.StringType(), False),  # md5
        T.StructField('airline', T.StringType(), False),
        T.StructField('flight_number', T.StringType(), False),
        T.StructField('port_id', T.StringType(), False),
        T.StructField('dst_airport_id', T.StringType(), False),
        T.StructField('dst_coordinates', T.StringType(), False),
        T.StructField('dst_longitude', T.DoubleType(), False),
        T.StructField('dst_latitude', T.DoubleType(), False),
        T.StructField('dst_city', T.StringType(), False),
        T.StructField('dst_port', T.StringType(), False),
        T.StructField('dst_airport_name', T.StringType(), False),
        T.StructField('dst_airport_international',
                      T.BooleanType(), False),
        T.StructField('dst_airport_type_id', T.IntegerType(), False),
        T.StructField('dst_airport_type', T.StringType(), False),
        T.StructField('dst_asian', T.DoubleType(), False),
        T.StructField('dst_black', T.DoubleType(), False),
        T.StructField('dst_latino', T.DoubleType(), False),
        T.StructField('dst_native', T.DoubleType(), False),
        T.StructField('dst_white', T.DoubleType(), False),
        T.StructField('dst_ethnicity_id', T.IntegerType(), False),
        T.StructField('dst_ethnicity', T.StringType(), False),
        T.StructField('dst_population', T.IntegerType(), False),
        T.StructField('dst_size_id', T.IntegerType(), False),
        T.StructField('dst_size', T.StringType(), False),
        T.StructField('dst_state_id', T.StringType(), False),
        T.StructField('dst_state_name', T.StringType(), False),
        T.StructField('dst_state_type_id', T.IntegerType(), False),
        T.StructField('dst_state_type', T.StringType(), False),
        T.StructField('dst_state_climate_id', T.IntegerType(), False),
        T.StructField('dst_state_climate', T.StringType(), False),
    ])
)

class RouteDimFile(DimFileBase):
    def __init__(self):
        super().__init__(
            "visitor_dim",
            schema,
        )

    nk = ['airline', 'flight_number', 'port_id']