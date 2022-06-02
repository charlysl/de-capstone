from pyspark.sql import SparkSession

import pyspark.sql.types as T
import pyspark.sql.functions as F

from datetime import datetime, timedelta
import math

from datalake.datamodel.age import Age
from datalake.datamodel.stay import Stay
from datalake.datamodel.files.sas_file import SasFile
from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.utils import spark_helper

"""
CLEANING:
April's SAS files has 6 extra columns which aren't needed
- this would prevent us from applyting schema on read
- the solution it to load without schema
- enforce schema by projecting with casts before saving parquet
"""


def load_sas_file(date):
    return SasFile(date).read()

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
            spark_helper.ifnull_str_expr('airline'),
            spark_helper.ifnull_str_expr('fltno', 'flight_number'),
            spark_helper.ifnull_str_expr('i94port', 'port_id'),
            spark_helper.ifnull_num_expr('i94cit', 'citizenship_id'),
            spark_helper.ifnull_num_expr('i94res', 'residence_id'),
            F.col('i94bir').cast('int').alias('age'), # not nk, ok if null
            convert_age_udf(F.expr('cast(i94bir as int)')).cast('int').alias('age_id'),
            spark_helper.ifnull_str_expr('gender', 'gender_id'),
            spark_helper.ifnull_num_expr('i94visa', 'visa_id'),
            spark_helper.ifnull_str_expr('i94addr', 'address_id'),
            (F.col('depdate') - F.col('arrdate')).cast('int').alias('stay'), # not nk, ok if null
            convert_stay_udf(F.col('arrdate'), F.col('depdate')).alias('stay_id'),
            F.lit('1').cast('int').alias('count') # take no chances with nulls
        )
)

def save_immigration(df):
    ImmigrationFile().stage(df)

def clean_immigration(date):
    return (
        load_sas_file(date)
        #.pipe(SparkETL.filter_one_month, '2016-12-01')
        .pipe(only_air)
        .pipe(project_schema)
        .pipe(save_immigration)
    )


clean_immigration(spark_helper.get_date())