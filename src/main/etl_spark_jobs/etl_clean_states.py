from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from datalake.datamodel.files.raw_states_file import RawStatesFile
from datalake.datamodel.files.states_file import StatesFile

import sys


def load_raw_states():
    return RawStatesFile().read()

def filter_missing_state_id(df):
    """
    Description: filter out those "states" that have no abbreviation
    """
    return (
        df
        .filter(F.col('Abbreviation').isNotNull())
    )

def clean_state_id(df):
    return df.withColumn('state_id', F.expr('TRIM(Abbreviation)'))

def clean_name(df):
    """
    Description: some state names have a "[E]" suffix; remove it
    """
    return df.withColumn('name', F.expr("""
        IF(
            SUBSTR(TRIM(Name), -1) = ']', 
            SUBSTRING_INDEX(TRIM(Name), '[', 1), 
            TRIM(Name)
        )
        """))

def clean_type_id(df):
    return df.withColumn('type_id', F.expr("""
          CASE TRIM(Type)
            WHEN 'State' THEN 0 
            WHEN 'Federal District' THEN 1
            WHEN 'Territory' THEN 2
          END
          """)
    )

def clean_type(df):
    return df.withColumn('type', F.expr('TRIM(Type)'))

def project_schema(df):
    """
    Description: select only the required columns
    """
    return df.select('state_id', 'name', 'type_id', 'type')

def save_clean_states(df):
    StatesFile().stage(df)

def clean_states():
    """
    Description: Spark job that cleans raw US states.

    Prerequisites: a states file exists in raw datalake area.

    Effects: a states file created in curated datalake area that:
    - has 50 states, 1 federal district and the 6 main territories
    """
    return (
        load_raw_states()
        .pipe(filter_missing_state_id)
        .pipe(clean_state_id)
        .pipe(clean_name)
        .pipe(clean_type_id)
        .pipe(clean_type)
        .pipe(project_schema)
        .pipe(save_clean_states)
)


clean_states()


