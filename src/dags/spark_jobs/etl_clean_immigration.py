import pyspark.sql.types as T
import pyspark.sql.functions as F

from datetime import datetime, timedelta

from etl import SparkETL
from age import Age
from stay import Stay

etl = SparkETL()
spark = etl.get_spark()

schema = T.StructType([
    T.StructField('_c0', T.StringType(), True),
    T.StructField('cicid', T.StringType(), True),
    T.StructField('i94yr', T.StringType(), True),
    T.StructField('i94mon', T.StringType(), True),
    T.StructField('i94cit', T.StringType(), True),
    T.StructField('i94res', T.StringType(), True),
    T.StructField('i94port', T.StringType(), True),
    T.StructField('arrdate', T.FloatType(), True),
    T.StructField('i94mode', T.FloatType(), True),
    T.StructField('i94addr', T.StringType(), True),
    T.StructField('depdate', T.FloatType(), True),
    T.StructField('i94bir', T.FloatType(), True),
    T.StructField('i94visa', T.StringType(), True),
    T.StructField('count', T.FloatType(), True),
    T.StructField('dtadfile', T.StringType(), True),
    T.StructField('visapost', T.StringType(), True),
    T.StructField('occup', T.StringType(), True),
    T.StructField('entdepa', T.StringType(), True),
    T.StructField('entdepd', T.StringType(), True),
    T.StructField('entdepu', T.StringType(), True),
    T.StructField('matflag', T.StringType(), True),
    T.StructField('biryear', T.StringType(), True),
    T.StructField('dtaddto', T.StringType(), True),
    T.StructField('gender', T.StringType(), True),
    T.StructField('insnum', T.StringType(), True),
    T.StructField('airline', T.StringType(), True),
    T.StructField('admnum', T.StringType(), True),
    T.StructField('fltno', T.StringType(), True),
    T.StructField('visatype', T.StringType(), True),
    
])

immigration_staging = (
    spark
    .read
    .format('csv')
    .schema(schema)
    .option('format', 'csv')
    .option('header', 'true')
    .load(etl.data_sources['immigration'])
)

immigration_staging.limit(1).toPandas()

sas_epoc = datetime(1960, 1, 1)

def convert_sas_date(arrdate):
    return sas_epoc + timedelta(days=arrdate)

@F.udf(T.DateType())
def convert_sas_date_udf(arrdate):
    return convert_sas_date(arrdate)



@F.udf(T.IntegerType())
def sas_date_to_day_udf(arrdate):
    return convert_sas_date(arrdate).day

@F.udf(T.IntegerType())
def convert_age_udf(age):
    return Age(age).group()

@F.udf(T.IntegerType())
def convert_stay_udf(arrdate, depdate):
    return Stay(arrdate, depdate).group()

def only_air(df):
    return df.where(F.col('i94mode') == 1)

def project_schema(df):
    return (
        df
        .select(
            # partition
            F.col('i94yr').cast('int').alias('year'),
            F.col('i94mon').cast('int').alias('month_id'),
            sas_date_to_day_udf(F.col('arrdate')).alias('day'),
            convert_sas_date_udf(F.col('arrdate')).alias('arrival_date'),
            'airline',
            F.col('fltno').alias('flight_number'),
            F.col('i94port').alias('port_id'),
            F.col('i94cit').cast('int').alias('citizenship_id'),
            F.col('i94res').cast('int').alias('residence_id'),
            F.col('i94bir').cast('int').alias('age'),
            convert_age_udf(F.col('i94bir')).alias('age_id'),
            F.col('gender').alias('gender_id'),
            F.col('i94visa').cast('int').alias('visa_id'),
            F.col('i94addr').alias('address_id'),
            (F.col('depdate') - F.col('arrdate')).cast('int').alias('stay'),
            convert_stay_udf(F.col('arrdate'), F.col('depdate')).alias('stay_id'),
            'count'
        )
)

def clean_immigration(df):
    return (
        immigration_staging
        .pipe(only_air)
        .pipe(project_schema)
    )

immigration = clean_immigration(immigration_staging)

etl.save_clean_table(immigration, 'immigration', partitions=['year', 'month_id'])