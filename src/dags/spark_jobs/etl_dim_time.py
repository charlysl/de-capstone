import pyspark.sql.functions as F

from etl import SparkETL
from dim import TimeDim

etl = SparkETL()
spark = etl.get_spark()
dim_helper = TimeDim()

immigration = etl.read_clean_table('immigration')

def time_dim_nk(df):
    return (
        df
        .select(
            'arrival_date'
        )
        .distinct()
    )

def join_immigration_time_dim(df):
    
    time_dim = etl.read_dim_table('time_dim')
    
    return (
        df
        .join(
            time_dim,
            on= dim_helper.on_nk(df, time_dim, df_keys=['arrival_date']),
            how='leftanti'
        )
    )

def fill_time_dim(df):
    return df.select(
        F.expr(dim_helper.gen_sk_expr(df_keys=['arrival_date'])).alias('time_id'),
        F.col('arrival_date').alias('date'),
        F.expr('YEAR(arrival_date)').alias('year'),
        F.expr('MONTH(arrival_date)').alias('month_id'),
        F.expr('DAY(arrival_date)').alias('day'),
        F.expr('WEEKDAY(arrival_date)').alias('weekday_id'),
        F.expr("DATE_FORMAT(arrival_date, 'MMM')").alias('month'),
        F.expr("DATE_FORMAT(arrival_date, 'E')").alias('weekday'),
        F.expr("DATE_FORMAT(arrival_date, 'E') IN ('Sat', 'Sun')").alias('weekend')
    )

etl.read_dim_table('time_dim').toPandas()

def time_dim_append(date):
    return (
        immigration
        .pipe(SparkETL.filter_one_month, date)
        .pipe(time_dim_nk)
        .pipe(join_immigration_time_dim)
        .pipe(fill_time_dim)
    )

etl.save_dim_table(time_dim_append(SparkETL.get_date()), 'time_dim')
