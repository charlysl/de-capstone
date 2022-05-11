import pyspark.sql.types as T
import pyspark.sql.functions as F

from etl import SparkETL

etl = SparkETL()
spark = etl.get_spark()

immigration = etl.read_clean_table('immigration')

def route_nk(df):
    return (
        df
        .select('airline', 'flight_number', 'port_id')
        .drop_duplicates()
    )

def missing_routes(df):
    
    route_dim = etl.read_dim_table('route_dim')
    
    return (
        df
        .join(
            route_dim,
            on=(
                (df['airline'] == route_dim['airline'])
                & (df['flight_number'] == route_dim['flight_number'])
                & (df['port_id'] == route_dim['port_id'])
            ),
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
    return df.withColumn('route_id', F.monotonically_increasing_id())

def fill_airport(df):
    
    airports = (
        etl.read_clean_table('airport')
        .withColumnRenamed('airport_id', 'dst_airport_id')
    )
    
    ports_to_airports = (
        etl.read_clean_table('port_to_airport')
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
    
    demographics = etl.read_clean_table('demographics')
    
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
    
    states = etl.read_clean_table('state')
    
    return (
        df
        .join(states, on=df['dst_state_id'] == states['state_id'], how='left')
        .drop('state_id')
        .withColumnRenamed('name', 'dst_state_name')
        .withColumnRenamed('type_id', 'dst_state_type_id')
        .withColumnRenamed('type', 'dst_state_type')
    )

def fill_temperature(df):
    
    temperatures = etl.read_clean_table('temperature')
    
    return (
        df
        .join(temperatures, on=df['dst_state_id'] == temperatures['state_id'], how='left')
        .drop('state_id')
        .withColumnRenamed('climate_id', 'dst_state_climate_id')
        .withColumnRenamed('climate', 'dst_state_climate')
    )

def fill_missing_routes(df):
    return (
        df
        .pipe(missing_routes)
        .pipe(route_nk)
        .pipe(fill_sk)
        .pipe(fill_airport)
        .pipe(fill_demographics)
        .pipe(fill_state)
        .pipe(fill_temperature)
    )

etl.save_dim_table(
    immigration.pipe(fill_missing_routes),
    'route_dim'
)

