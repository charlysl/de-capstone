from pyspark.sql import DataFrame
import pyspark.sql.types as T

from etl import SparkETL

etl = SparkETL()
spark = etl.get_spark()

schema = T.StructType([
    T.StructField('time_id', T.StringType(), False),
    T.StructField('route_id', T.StringType(), False),
    T.StructField('visitor_id', T.StringType(), False),
    T.StructField('num_visitors', T.LongType(), False),
    T.StructField('age_avg', T.DoubleType(), True),
    T.StructField('age_std', T.DoubleType(), True),
    T.StructField('stay_avg', T.DoubleType(), True),
    T.StructField('stay_std', T.DoubleType(), True),
])

def empty_dataframe(schema):
    return spark.createDataFrame([], schema)

etl.init_fact_table(
    empty_dataframe(schema),
    'flight_fact'
)