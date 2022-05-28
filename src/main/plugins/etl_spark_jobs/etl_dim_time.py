import pyspark.sql.functions as F

from datalake.datamodel.files.immigration_file import ImmigrationFile

from datalake.datamodel.files.time_dim_file import TimeDimFile
time_dim_file = TimeDimFile()

from datalake.utils import spark_helper


def load_immigration():
    return ImmigrationFile().read()

def time_dim_nk(df):
    return (
        df
        .select(
            'arrival_date'
        )
        .distinct()
    )

def join_immigration_time_dim(df):
    
    time_dim_df = TimeDimFile().read()
    
    return (
        df
        .join(
            time_dim_df,
            on= time_dim_file.on_nk(df, time_dim_df, df_keys=['arrival_date']),
            how='leftanti'
        )
    )

def fill_time_dim(df):
    return df.select(
        F.expr(time_dim_file.gen_sk_expr(df_keys=['arrival_date'])).alias('time_id'),
        F.col('arrival_date').alias('date'),
        F.expr('YEAR(arrival_date)').alias('year'),
        F.expr('MONTH(arrival_date)').alias('month_id'),
        F.expr('DAY(arrival_date)').alias('day'),
        F.expr('WEEKDAY(arrival_date)').alias('weekday_id'),
        F.expr("DATE_FORMAT(arrival_date, 'MMM')").alias('month'),
        F.expr("DATE_FORMAT(arrival_date, 'E')").alias('weekday'),
        F.expr("DATE_FORMAT(arrival_date, 'E') IN ('Sat', 'Sun')").alias('weekend')
    )

def save_time_dim(df):
    TimeDimFile().save(df, area='staging')

def upsert_time_dim(date):
    return (
        load_immigration()
        .pipe(spark_helper.filter_one_month, date)
        .pipe(time_dim_nk)
        .pipe(join_immigration_time_dim)
        .pipe(fill_time_dim)
        .pipe(save_time_dim)
    )

upsert_time_dim(spark_helper.get_date())