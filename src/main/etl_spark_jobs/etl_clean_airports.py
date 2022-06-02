from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

import pandas as pd
import sys

from datalake.datamodel.files.raw_airports_file import RawAirportsFile
from datalake.datamodel.files.airports_file import AirportsFile
from datalake.datamodel.files.states_file import StatesFile

from datalake.utils import spark_helper


def load_airports():
    return RawAirportsFile().read()

def filter_us_iso_countries(df):
    
    us_iso_countries_pd = pd.DataFrame(
        {'iso_country2': ['US', 'AS', 'FM', 'GU', 'MH', 'MP', 'PR', 'PW', 'VI']}
    )
    
    us_iso_countries = spark_helper.get_spark().createDataFrame(us_iso_countries_pd)
    
    return (
        df
        .join(
            us_iso_countries,
            on=df['iso_country'] == us_iso_countries['iso_country2'],
            how='inner'
        )
        .drop('iso_country2')
    )


def drop_null_iata(df):
    """
    Assume that only international airports will have an IATA code, given
    that IATA stands for "International Air Transport Association".
    """
    return df.where(F.expr('iata_code IS NOT NULL'))

def drop_closed(df):
    return df.where(F.col('type') != 'closed')

def drop_unknown_states(df):
    """
    After dropping null iatas and closed airports, there are still unknown state ids:
    - MH (Marshall Islands, an independent country, but somehow passed the US-state filter),
    - FM (Micronesia)
    - PW (Palau)
    These are all independent countries, so drop them
    """
    
    states = StatesFile().read(area='staging').select('state_id')
    
    return (
        df.join(states, on='state_id', how='inner')
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
            F.col('iata_code').alias('airport_iata'),
            'state_id',
            F.col('municipality').alias('city'),
            'name',
            'international',
            'type_id',
            'type',
            'coordinates'
        )
    )

def save_clean_airports(df):    
    AirportsFile().stage(df)
    
def clean_airports():
    return (
        load_airports()
        .pipe(filter_us_iso_countries)
        .pipe(drop_null_iata)
        .pipe(drop_closed)
        .pipe(project_state)
        .pipe(drop_unknown_states)
        .pipe(project_type_id)
        .pipe(project_international)
        .pipe(project_schema)
        .pipe(save_clean_airports)
    )

clean_airports()