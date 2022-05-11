import pyspark.sql.functions as F

from etl import SparkETL

etl = SparkETL()
spark = etl.get_spark()

immigration = etl.read_clean_table('immigration')

time_dim = etl.read_dim_table('time_dim')

def time_dim_nk(df):
    return (
        df
        .select(
            'arrival_date'
        )
        .distinct()
    )

def join_immigration_time_dim(nk1, nk2):
    return (
        nk1
        .join(
            nk2,
            on=(
                (nk1['arrival_date'] == nk2['date'])
              ),
            how='leftanti'
        )
    )

def fill_time_dim(df):
    return df.select(
        F.monotonically_increasing_id().alias('time_id'),
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

time_dim_append = (
    immigration
    .pipe(time_dim_nk)
    .pipe(join_immigration_time_dim, time_dim)
    .pipe(fill_time_dim)
)

etl.save_dim_table(time_dim_append, 'time_dim')