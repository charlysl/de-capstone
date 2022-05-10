from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from etl import SparkETL

states_schema = (
    T.StructType([
        T.StructField('Type', T.StringType(), True),
        T.StructField('Name', T.StringType(), True),
        T.StructField('Abbreviation', T.StringType(), True),
        T.StructField('Capital', T.StringType(), True),
        T.StructField('Population (2015)', T.StringType(), True),
        T.StructField('Population (2019)', T.StringType(), True),
        T.StructField('area (square miles)', T.StringType(), True),
    ])
)

def load_states_staging(etl):
    return  (
        etl.get_spark()
        .read
        .format('csv')
        .schema(states_schema)
        .option('encoding', 'ISO-8859-1')
        .option('header', 'true')
        .load(etl.data_sources['states'])
    )

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

def save_clean_states(df, etl):
    etl.save_clean_table(
        df,
        'state'
    )

def clean_states():
    etl = SparkETL('etl-clean-states')
    return (
        load_states_staging(etl)
        .pipe(filter_missing_state_id)
        .pipe(clean_state_id)
        .pipe(clean_name)
        .pipe(clean_type_id)
        .pipe(clean_type)
        .pipe(project_schema)
        .pipe(save_clean_states, etl)
)

clean_states()

