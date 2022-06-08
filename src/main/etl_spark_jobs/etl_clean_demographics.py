from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT

import pyspark.sql.types as T
import pyspark.sql.functions as F

import sys


from datalake.datamodel.files.raw_demographics_file import RawDemographicsFile
from datalake.datamodel.files.demographics_file import DemographicsFile
from datalake.datamodel.files.states_file import StatesFile

from datalake.utils import spark_helper


def load_demographics():
    return RawDemographicsFile().read()

def race_cols(df):
    return df.select('City', 'State', 'Race', 'Count', 'Total Population')

def race_ratio(df):
    return df.withColumn('race_ratio', F.col('Count') / F.col('Total Population'))

def race_ratios(df):
    return (
        df
        .pipe(race_cols)
        .pipe(race_ratio)
    )

def pivot_race(df):
    race_ratios(df).createOrReplaceTempView('demographics')
    
    return (
        spark_helper.get_spark().sql("""
                SELECT 
                    City, 
                    State, 
                    `Total Population` as population,
                    FIRST(Count) as count, 
                    IFNULL(MAX(native), 0.0) AS native,
                    IFNULL(MAX(asian), 0.0) AS asian,
                    IFNULL(MAX(black), 0.0) AS black,
                    IFNULL(MAX(latino), 0.0) AS latino,
                    IFNULL(MAX(white), 0.0) AS white
                FROM demographics
                PIVOT (
                    MAX(race_ratio) as ratio
                    FOR Race IN (
                        'American Indian and Alaska Native' AS native, 
                        'Asian' AS asian, 
                        'Black or African-American' AS black, 
                        'Hispanic or Latino' AS latino, 
                        'White' AS white
                    )
                )
                GROUP BY City, State, `Total Population`
        """)
        .drop('count')
    )

@F.udf(VectorUDT())
def features_udf(ratios_array):
    return Vectors.dense(ratios_array)

def prepare_features(df):
    return df.withColumn(
        'features', 
        features_udf(
            F.array(
                F.col('native'),
                F.col('asian'),
                F.col('black'),
                F.col('latino'),
                F.col('white')))
    )

def ratios_kmeans(df):
    kmeans = KMeans().setK(7).setSeed(1)
    model = kmeans.fit(df)
    return model.transform(df)

def project_clusters(df):
    return (
        load_demographics()
        .pipe(race_cols)
        .pipe(race_ratio)
        .pipe(pivot_race)
        .pipe(prepare_features)
        .pipe(ratios_kmeans)
    )

def ethnicities():
    return spark_helper.get_spark().createDataFrame([
                                    [0, 'white, black minority'],
                                    [6, 'white'],    
                                    [2, 'white, latino minority'],    
                                    [5, 'white, asian minority'],
                                    [4, 'black, white minority'],    
                                    [1, 'latino, white minority'],    
                                    [3, 'white latino']
                        ],
        # need explicit schema, otherwise ethnicity_id will be type long
        schema=T.StructType([
            T.StructField('ethnicity_id', T.IntegerType(), True),
            T.StructField('ethnicity', T.StringType(), True)
        ])
    )
    

def join_ethnicities(df):
    ethnicities_df = ethnicities()
    return df.join(
        ethnicities_df,
        on=ethnicities_df['ethnicity_id'] == df['prediction'],
        how='inner'
    )

def join_state(df):
    
    state = StatesFile().read()

    return df.join(
        state,
        on=df['State'] == state['name'],
        how='left'
    )

@F.udf(T.IntegerType())
def size_id_udf(population):
    if population < 200000:
        return 0
    elif population <= 500000:
        return 1
    elif population <= 1500000:
        return 2
    else:
        return 3

@F.udf(T.StringType())
def size_udf(population):
    if population < 200000:
        return 'small (50K - 200K)'
    elif population <= 500000:
        return 'medium (200K - 500K)'
    elif population <= 1500000:
        return 'large (500K - 1,5M)'
    else:
        return 'very large (> 1,5M)'

def project_size(df):
    return (
        df
        .withColumn('size_id', size_id_udf(F.col('population')))
        .withColumn('size', size_udf(F.col('population')))
    )

def project_schema(df):
    return (
        df.select(
            'state_id',
            F.col('City').alias('city'),
            'asian',
            'black',
            'latino',
            'native',
            'white',
            'ethnicity_id',
            'ethnicity',
            'population',
            'size_id',
            'size'
        )
    )

def clean_demographics():
    return (
        load_demographics()
        .pipe(project_clusters)
        .pipe(join_ethnicities)
        .pipe(join_state)
        .pipe(project_size)
        .pipe(project_schema)
        .pipe(save_demographics)
    )

def save_demographics(df):
    DemographicsFile().stage(df)


clean_demographics()