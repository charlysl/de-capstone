import pyspark.sql.functions as F


from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.flight_fact_file import FlightFactFile
from datalake.datamodel.files.route_dim_file import RouteDimFile
from datalake.datamodel.files.visitor_dim_file import VisitorDimFile
from datalake.datamodel.files.time_dim_file import TimeDimFile

from datalake.utils import spark_helper


def load_immigration():
    return ImmigrationFile().read()

def aggregates(df):
    return (
        df
        .withColumn('day', F.expr("DAY(arrival_date)"))
        .groupby(
            'arrival_date', # time_dim nk
            *(RouteDimFile().get_nk()),
            *(VisitorDimFile().get_nk())
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
    
    time_dim = TimeDimFile().read(area='staging')
    
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
    
    route_dim = RouteDimFile().read(area='staging')
    
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
    
    visitor_dim = VisitorDimFile().read(area='staging')
    
    return (
        df
        .join(
            visitor_dim.select('visitor_id', *(VisitorDimFile().get_nk())),
            VisitorDimFile().on_nk(df, visitor_dim),
            'inner'
        )
        .drop(*(VisitorDimFile().get_nk()))
    )

def project_schema(df):
    
    column_order = ['time_id', 'route_id', 'visitor_id', 'num_visitors', 'age_avg', 'age_std', 'stay_avg', 'stay_std']
    
    return df.select(*column_order)

def save_flight_fact(df):
    FlightFactFile().stage(df)

def etl_flight_fact(date):
    """
    Description:    Append flight measures for given year and month to
                    flight fact file.
    Parameters:
        date: str - a string with format 'yyyy-mm-dd'
    Preconditions:
    - a clean immigration file with records for given year and month
    - time, route and visitor dimensions that preserve referential integrity
    Effects:
    - measures for given year and month appended to flight fact file
    """

    return (
        load_immigration()
        .pipe(spark_helper.filter_one_month, date)
        .pipe(aggregates)
        #.pipe(replace_null_stddev, 'age_std')
        #.pipe(replace_null_stddev, 'stay_std')
        .pipe(project_time_sk)
        .pipe(project_route_sk)
        .pipe(project_visitor_sk)
        .pipe(project_schema)
        .pipe(save_flight_fact)
    )



etl_flight_fact(spark_helper.get_date())