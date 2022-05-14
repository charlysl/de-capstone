from pyspark.sql import SparkSession

import pyspark.sql.types as T
import pyspark.sql.functions as F

from datetime import datetime, timedelta
import math

from etl import SparkETL
from age import Age
from stay import Stay

"""
CLEANING:
April's SAS files has 6 extra columns which aren't needed
- this would prevent us from applyting schema on read
- the solution it to load without schema
- enforce schema by projecting with casts before saving parquet
"""

etl = SparkETL()

spark = (
    SparkSession
    .builder
    .appName("de-capstone")
    .config('spark.jars.repositories', 'https://repos.spark-packages.org/')
    .config('spark.jars.packages', 'saurfang:spark-sas7bdat:3.0.0-s_2.12')
    .getOrCreate()
)

dir = '/Users/charly/DataEng2022/de-capstone/data/18-83510-I94-Data-2016'

def parse_date(date):
    return datetime.strptime(date, '%Y-%m-%d')

def sas_file_path(date):
    """
    Example: 'i94_jan16_sub.sas7bdat'
    """
    year = datetime.strftime(date, '%y')
    month = datetime.strftime(date, '%b').lower()
    file = f"i94_{month}{year}_sub.sas7bdat"
    return f"{dir}/{file}"

def read_sas_file(path):
    # see https://stackoverflow.com/questions/35684856/import-pyspark-packages-with-a-regular-jupyter-notebook
    return (
        spark
        .read
        .format('com.github.saurfang.sas.spark')
        #.schema(schema)
        .load(path)
    )

def immigration_staging(date):
    return (
        read_sas_file(
            sas_file_path(
                parse_date(date)
            )
        )
    )

@F.udf(T.IntegerType())
def convert_age_udf(age):
    return Age(age).group()

@F.udf(T.IntegerType())
def convert_stay_udf(arrdate, depdate):
    return Stay(arrdate, depdate).group()

def only_air(df):
    return df.where(F.col('i94mode') == 1)

# sas date format epoc is 1st January 1960
convert_sas_date_expr = "date_add(date('1960-01-01'), cast(arrdate as int))"

def project_schema(df):
    return (
        df
        .select(
            F.expr(f"year({convert_sas_date_expr})").alias('year'),
            F.expr(f"month({convert_sas_date_expr})").alias('month_id'),
            F.expr(f"day({convert_sas_date_expr})").alias('day'),
            F.expr(convert_sas_date_expr).alias('arrival_date'),
            SparkETL.ifnull_str_expr('airline'),
            SparkETL.ifnull_str_expr('fltno', 'flight_number'),
            SparkETL.ifnull_str_expr('i94port', 'port_id'),
            SparkETL.ifnull_num_expr('i94cit', 'citizenship_id'),
            SparkETL.ifnull_num_expr('i94res', 'residence_id'),
            F.col('i94bir').cast('int').alias('age'), # not nk, ok if null
            convert_age_udf(F.expr('cast(i94bir as int)')).cast('int').alias('age_id'),
            SparkETL.ifnull_str_expr('gender', 'gender_id'),
            SparkETL.ifnull_num_expr('i94visa', 'visa_id'),
            SparkETL.ifnull_str_expr('i94addr', 'address_id'),
            (F.col('depdate') - F.col('arrdate')).cast('int').alias('stay'), # not nk, ok if null
            convert_stay_udf(F.col('arrdate'), F.col('depdate')).alias('stay_id'),
            F.lit('1').cast('int').alias('count') # take no chances with nulls
        )
)

def clean_immigration(df):
    return (
        df
        #.pipe(SparkETL.filter_one_month, '2016-12-01')
        .pipe(only_air)
        .pipe(project_schema)
    )

def save_immigration(df):
    etl.save_clean_table(
        clean_immigration(df),
        'immigration',
        partitions=['year', 'month_id'],
        mode='append'
    )

save_immigration(
    immigration_staging(
        SparkETL.get_date()))
