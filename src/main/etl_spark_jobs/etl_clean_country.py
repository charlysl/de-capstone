import pyspark.sql.functions as F

import sys

from datalake.datamodel.files.i94_data_dictionary_file import I94DataDictionaryFile
from datalake.datamodel.files.country_file import CountryFile


def load_i94_data_dictionary():
    return I94DataDictionaryFile().read()

def explode_countries(df):
    return (
        df
        .select(
            F.explode('I94CIT')
        )
        .select(
            F.element_at(F.col('col'), 1).cast('int').alias('country_id'),
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

def save_clean_country(df):
    CountryFile().save(df)

def clean_country():
    return (
        load_i94_data_dictionary()
        .pipe(explode_countries)
        .pipe(clean_mexico)
        .pipe(save_clean_country)
    )


clean_country()