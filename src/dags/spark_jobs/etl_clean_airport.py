from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

import pandas as pd

from etl import SparkETL

etl = SparkETL()
spark = etl.get_spark()

airport_schema = T.StructType([
    T.StructField('ident', T.StringType(), True),
    T.StructField('type', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('elevation_ft', T.StringType(), True),
    T.StructField('continent', T.StringType(), True),
    T.StructField('iso_country', T.StringType(), True),
    T.StructField('iso_region', T.StringType(), True),
    T.StructField('municipality', T.StringType(), True),
    T.StructField('gps_code', T.StringType(), True),
    T.StructField('iata_code', T.StringType(), True),
    T.StructField('local_code', T.StringType(), True),
    T.StructField('coordinates', T.StringType(), True)
])

airport_staging = (
    spark
    .read
    .format('csv')
    .schema(airport_schema)
    .option('header', 'true')
    .load(etl.data_sources['airports'])
)



def filter_us_iso_countries(df):
    
    us_iso_countries_pd = pd.DataFrame(
        {'iso_country2': ['US', 'AS', 'FM', 'GU', 'MH', 'MP', 'PR', 'PW', 'VI']}
    )
    
    us_iso_countries = spark.createDataFrame(us_iso_countries_pd)
    
    return (
        df
        .join(
            us_iso_countries,
            on=df['iso_country'] == us_iso_countries['iso_country2'],
            how='inner'
        )
        .drop('iso_country2')
    )

def project_state(df):
    return (
        df
        .withColumn('state_id', F.expr("""
            IF(
                SUBSTR(iso_region, 0, 2) = 'US',
                SUBSTR(iso_region, 4),
                SUBSTR(iso_region, 0, 2)
            )
        """))
    )

def project_type_id(df):
    return df.withColumn('type_id', F.expr("""
                            CASE type
                                WHEN 'closed' THEN 0
                                WHEN 'balloonport' THEN 1
                                WHEN 'heliport' THEN 2
                                WHEN 'seaplane_base' THEN 3
                                WHEN 'small_airport' THEN 4
                                WHEN 'medium_airport' THEN 5
                                WHEN 'large_airport' THEN 6
                            END
                        """)
                         )

def project_international(df):
    return df.withColumn(
        'international',
        F.expr("LOWER(name) LIKE '%international%'")
    )

def project_schema(df):
    return (
        df
        .select(
            F.col('ident').alias('airport_id'),
            'state_id',
            F.col('municipality').alias('city'),
            'name',
            'international',
            'type_id',
            'type',
            'coordinates'
        )
    )

def clean_airport(df):
    return (
        airport_staging
        .pipe(filter_us_iso_countries)
        .pipe(project_state)
        .pipe(project_type_id)
        .pipe(project_international)
        .pipe(project_schema)
    )

etl.save_clean_table(airport_staging.pipe(clean_airport), 'airport')

