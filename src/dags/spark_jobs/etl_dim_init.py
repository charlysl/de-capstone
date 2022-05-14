from pyspark.sql import DataFrame
import pyspark.sql.types as T

from etl import SparkETL

etl = SparkETL()

spark = etl.get_spark()

# Initialize all dimension tables; must be idempotent, like CREATE IF NOT EXISTS

dimensions = {
    'time_dim': (
            T.StructType([
                T.StructField('time_id', T.StringType(), False), # md5
                T.StructField('date', T.DateType(), False),
                T.StructField('year', T.IntegerType(), False),
                T.StructField('month_id', T.IntegerType(), False),
                T.StructField('day', T.IntegerType(), False),
                T.StructField('weekday_id', T.IntegerType(), False),
                T.StructField('month', T.StringType(), False),
                T.StructField('weekday', T.StringType(), False),
                T.StructField('weekend', T.BooleanType(), False)
            ])
    ),
    
    'foreign_visitor_dim': (
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
    ),
  
    'route_dim': (
            T.StructType([
                T.StructField('route_id', T.StringType(), False), # md5
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
                T.StructField('dst_airport_international', T.BooleanType(), False),
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
}

def empty_dataframe(schema):
    return spark.createDataFrame([], schema)

def init_dims(dims):
    for dim, schema in dimensions.items():
        etl.init_dim_table(
            empty_dataframe(schema),
            dim
        )

init_dims(dimensions)

