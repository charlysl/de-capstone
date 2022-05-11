import pyspark.sql.functions as F

from etl import SparkETL

etl = SparkETL()

i94_data_dictionary = etl.read_clean_table('i94_data_dictionary')

def explode_countries(df):
    return (
        df
        .select(
            F.explode('I94CIT')
        )
        .select(
            F.element_at(F.col('col'), 1).alias('country_id'),
            F.element_at(F.col('col'), 2).alias('country')
        )
    )

def clean_mexico(df):
    return (
        df
        .withColumn(
            'country',
            F.expr("""
                IF(
                    country_id = 582,
                    'MEXICO',
                    country
                )
            """)
        )
    )

def clean_country(df):
    return (
        i94_data_dictionary
        .pipe(explode_countries)
        .pipe(clean_mexico)
    )

def save_clean_country():
    etl.save_clean_table(i94_data_dictionary.pipe(clean_country), 'country')

save_clean_country()
