import pyspark.sql.types as T
import pyspark.sql.functions as F

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.route_dim_file import RouteDimFile

from datalake.datamodel.files.airports_file import AirportsFile
from datalake.datamodel.files.ports_to_airports_file import PortsToAirportsFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.demographics_file import DemographicsFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile


from datalake.utils import spark_helper


def load_immigration():
    return ImmigrationFile().read()

def route_nk(df):
    return (
        df
        .select(RouteDimFile().get_nk())
        .drop_duplicates()
    )

def missing_routes(df):
    
    route_dim = RouteDimFile().read()
    
    return (
        df
        .join(
            route_dim,
            on=RouteDimFile().on_nk(df, route_dim),
            how='leftanti'
        )
    )

def coordinate_expr(index):
    return F.expr(f"""
            CAST(
                ELEMENT_AT(
                    SPLIT(dst_coordinates, ','), 
                    {index}
                ) 
                AS DOUBLE
            )
        """)

def fill_sk(df):
    return df.withColumn('route_id', F.expr(RouteDimFile().gen_sk_expr()))

def fill_airport(df):
    
    airports = (
        AirportsFile().read()
        .withColumnRenamed('airport_id', 'dst_airport_id')
    )
    
    ports_to_airports = (
        PortsToAirportsFile().read()
        .withColumnRenamed('port_id', 'airport_port_id')
    )
    
    return (
        df
        .join(
            ports_to_airports,
            on=df['port_id'] == ports_to_airports['airport_port_id'],
            how='left'
        )
        .drop('airport_port_id')
        .join(
            airports,
            on=ports_to_airports['airport_id'] == airports['dst_airport_id'],
            how='left'
        )
        .drop('airport_id')
        .withColumnRenamed('city', 'dst_city')
        .withColumnRenamed('state_id', 'dst_state_id')
        .withColumnRenamed('name', 'dst_airport_name')
        .withColumnRenamed('international', 'dst_airport_international')
        .withColumnRenamed('type_id', 'dst_airport_type_id')
        .withColumnRenamed('type', 'dst_airport_type')
        .withColumnRenamed('coordinates', 'dst_coordinates')
        .withColumn('dst_longitude', coordinate_expr(1))
        .withColumn('dst_latitude', coordinate_expr(2))
    )

def fill_demographics(df):
    
    demographics = DemographicsFile().read()
    
    return (
        df
        .join(
            demographics,
            on=(
                (df['dst_state_id'] == demographics['state_id'])
                & (df['dst_city'] == demographics['city'])
            ),
            how='left'
        )
        .drop('state_id', 'city')
        .withColumnRenamed('asian', 'dst_asian')
        .withColumnRenamed('black', 'dst_black')
        .withColumnRenamed('latino', 'dst_latino')
        .withColumnRenamed('native', 'dst_native')
        .withColumnRenamed('white', 'dst_white')
        .withColumnRenamed('ethnicity_id', 'dst_ethnicity_id')
        .withColumnRenamed('ethnicity', 'dst_ethnicity')
        .withColumnRenamed('population', 'dst_population')
        .withColumnRenamed('size_id', 'dst_size_id')
        .withColumnRenamed('size', 'dst_size')
    )

def fill_state(df):
    
    states = StatesFile().read()
    
    return (
        df
        .join(states, on=df['dst_state_id'] == states['state_id'], how='left')
        .drop('state_id')
        .withColumnRenamed('name', 'dst_state_name')
        .withColumnRenamed('type_id', 'dst_state_type_id')
        .withColumnRenamed('type', 'dst_state_type')
    )

def fill_temperature(df):
    
    temperatures = TemperaturesFile().read()
    
    return (
        df
        .join(temperatures, on=df['dst_state_id'] == temperatures['state_id'], how='left')
        .drop('state_id')
        .withColumnRenamed('climate_id', 'dst_state_climate_id')
        .withColumnRenamed('climate', 'dst_state_climate')
    )

def save_route_dim(df):
    RouteDimFile().save(df, area=RouteDimFile.staging)

def fill_missing_routes(date):
    return (
        load_immigration()
        .pipe(spark_helper.filter_one_month, date)
        .pipe(route_nk)
        .pipe(missing_routes)
        .pipe(fill_sk)
        .pipe(fill_airport)
        .pipe(fill_demographics)
        .pipe(fill_state)
        .pipe(fill_temperature)
    )

fill_missing_routes(spark_helper.get_date())
