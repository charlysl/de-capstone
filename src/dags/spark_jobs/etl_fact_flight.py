import pyspark.sql.functions as F

from etl import SparkETL
from dim import RouteDim, VisitorDim


etl = SparkETL()
spark = etl.get_spark()
visitor_helper = VisitorDim()

immigration = etl.read_clean_table('immigration')

def aggregates(df):
    return (
        df
        .withColumn('day', F.expr("DAY(arrival_date)"))
        .groupby(
            'arrival_date', # time_dim nk
            *(RouteDim().get_nk()),
            *(visitor_helper.get_nk())
        )
        .agg(
            F.count('count').alias('num_visitors'),
            F.avg('age').alias('age_avg'),
            F.stddev('age').alias('age_std'),
            F.avg('stay').alias('stay_avg'),
            F.stddev('stay').alias('stay_std'),
        )
    )

def replace_null(colname, value):
    return F.expr(f"""
            IF(
                {colname} IS NULL,
                {value},
                {colname}
            )
            """)



def replace_null_stddev(df, colname):
    return df.withColumn(
        colname,
        replace_null(colname, 0.0)
    )

def project_time_sk(df):
    
    time_dim = etl.read_dim_table('time_dim')
    
    return (
        df
        .join(
            time_dim.select('time_id', 'date'),
            on=df['arrival_date'] == time_dim['date'],
            how='inner'
        )
        .drop('arrival_date', 'date')
    )

def project_route_sk(df):
    
    route_dim = etl.read_dim_table('route_dim')
    
    return (
        df
        .join(
            route_dim.select('route_id', 'airline', 'flight_number', 'port_id'),
            on=(
                (df['airline'] == route_dim['airline'])
                & (df['flight_number'] == route_dim['flight_number'])
                & (df['port_id'] == route_dim['port_id'])
            ),
            how='inner'
        )
        .drop('airline', 'flight_number', 'port_id')
    )

def project_visitor_sk(df):
    
    visitor_dim = etl.read_dim_table('foreign_visitor_dim')
    
    return (
        df
        .join(
            visitor_dim.select('visitor_id', *(visitor_helper.get_nk())),
            visitor_helper.on_nk(df, visitor_dim),
            'inner'
        )
        .drop(*(visitor_helper.get_nk()))
    )

def project_schema(df):
    
    column_order = ['time_id', 'route_id', 'visitor_id', 'num_visitors', 'age_avg', 'age_std', 'stay_avg', 'stay_std']
    
    return df.select(*column_order)

def flight_fact(df, date):
    return (
        df
        .pipe(SparkETL.filter_one_month, date)
        .pipe(aggregates)
        #.pipe(replace_null_stddev, 'age_std')
        #.pipe(replace_null_stddev, 'stay_std')
        .pipe(project_time_sk)
        .pipe(project_route_sk)
        .pipe(project_visitor_sk)
        .pipe(project_schema)
    )

def etl_fact_flight(date):
    etl.save_fact_table(
        immigration.pipe(flight_fact, date),
        'flight_fact'
    )

etl_fact_flight(SparkETL.get_date())