from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from datetime import datetime
import sys

from datalake.utils.aws_helper import AwsHelper


spark = [None]

def get_spark():
    """
    It is important that this is the only place where a Spark
    shell session is configured for unit testing.
    It is different when submitting jobs to a Spark cluster, as
    is done by the Airflow SparkSubmitOperator; in that case,
    Spark will be configured by command line arguments.

    appName will be ignored if executed inside a cluster (submit),
    but will be honoured when executed in a shell (findspark)
    TODO: config parameter

    S3

    To prevent exceptions, must set the right hadoop version, at the end of
    ```
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.0")
    ```

    This must be the same as the one for 
    ```
    $SPARK_HOME/jars/hadoop-common-3.2.0.jar
    ```    
    """
    if not spark[0]:
        builder = (
            SparkSession.builder.appName('de-capstone')
            .config('spark.sql.shuffle.partitions', 5)
            .config('spark.executor.memory', '1g')
        )

        spark_config(builder)

        spark[0] = builder.getOrCreate()
        
    return spark[0]



def ifnull_str_expr(col, alias=None):
    return (
        F.expr(f"IF({col} IS NULL, 'UNKNOWN', {col})")
        .alias(alias if alias else col)
    )

def ifnull_num_expr(col, alias=None):
    return (
        F.expr(f"IF({col} IS NULL, 9999, CAST({col} AS INT))")
        .alias(alias if alias else col)
    )

def date_to_datetime(date):
    return datetime.strptime(date, '%Y-%m-%d')

def filter_one_month(df, date):
    """
    Filter out the rows that are not in the given year and month.
    
    Parameters:
    - df: an immigration Spark DataFrame
    - date: a string with format 'YYYY-MM-DD'
    
    Returns: a Spark DataFrame with only the rows in the given year and month.
    """

    dt = date_to_datetime(date)

    return (
        df
        .where(
            (F.col('year') == dt.year)
            & (F.col('month_id') == dt.month)
        )
    )

def get_date():
    return sys.argv[1]

def spark_config(builder=None):
    """
    Description: Spark configuration.

    Parameters: a spark builder.

    Returns:
    - If builder is given: None, but the builder is configured.
    - Otherwise: a dict with spark configuration.
    """
    spark_packages = [
        'saurfang:spark-sas7bdat:3.0.0-s_2.12',
        'org.apache.hadoop:hadoop-aws:3.2.0'
    ]

    config = {
        'spark.jars.repositories': 'https://repos.spark-packages.org/',
        'spark.jars.packages': ','.join(spark_packages),
        'spark.hadoop.fs.s3a.access.key': AwsHelper.access_key(),
        'spark.hadoop.fs.s3a.secret.key': AwsHelper.secret_key()
    }

    if builder:
        for key in config:
            builder.config(key, config[key])
    else:
        return config

