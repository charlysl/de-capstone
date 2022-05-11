import pyspark.sql.functions as F

from etl import SparkETL

etl = SparkETL()
spark = etl.get_spark()

immigration = etl.read_clean_table('immigration')

visitor_dim_nk = ['citizenship_id', 'residence_id', 'age_id', 'gender_id', 'visa_id', 'stay_id', 'address_id']

def aggregates(df):
    return (
        df
        .withColumn('day', F.expr("DAY(arrival_date)"))
        .groupby(
            
            'arrival_date', # time_dim nk
            
            'airline', 'flight_number', 'port_id', # route_dim nk
            
            *visitor_dim_nk
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

def project_visitor_sk_on(df, visitor_dim, keys=visitor_dim_nk):
    result = (df[visitor_dim_nk[0]] == visitor_dim[visitor_dim_nk[0]])
    for col in keys[1:]:
        result = result & (df[col] == visitor_dim[col])
    return result

def project_visitor_sk(df):
    
    visitor_dim = etl.read_dim_table('foreign_visitor_dim')
    
    return (
        df
        .join(
            visitor_dim.select('visitor_id', *visitor_dim_nk),
            project_visitor_sk_on(df, visitor_dim, visitor_dim_nk),
            'inner'
        )
        .drop(*visitor_dim_nk)
    )

def project_schema(df):
    
    column_order = ['time_id', 'route_id', 'visitor_id', 'num_visitors', 'age_avg', 'age_std', 'stay_avg', 'stay_std']
    
    return df.select(*column_order)

def flight_fact(df):
    return (
        immigration
        .pipe(aggregates)
        .pipe(replace_null_stddev, 'age_std')
        .pipe(replace_null_stddev, 'stay_std')
        .pipe(project_time_sk)
        .pipe(project_route_sk)
        .pipe(project_visitor_sk)
        .pipe(project_schema)
    )

etl.save_fact_table(
    immigration.pipe(flight_fact),
    'flight_fact'
)