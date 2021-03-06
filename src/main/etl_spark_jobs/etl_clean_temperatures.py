from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT

import pyspark.sql.functions as F
import pyspark.sql.types as T

from datalake.datamodel.files.raw_temperatures_file import RawTemperaturesFile 
from datalake.datamodel.files.temperatures_file import TemperaturesFile
from datalake.datamodel.files.states_file import StatesFile

from datalake.utils import spark_helper


def load_temperatures():
    return RawTemperaturesFile().read()

def filter_us_temperatures(df):
    return (
        df
        .where(F.col('Country') == 'United States')
        .drop('Country', 'AverageTemperatureUncertainty')
        .withColumnRenamed('dt', 'date')
        .withColumnRenamed('AverageTemperature', 'temperature')
    )

def filter_2012_temperatures(df):
    return df.where(F.col('dt').between('2012-01-01', '2012-12-31'))

def project_months(df):
    return df.withColumn('month', F.month(F.col('date')))

def pivot_temperatures(df):
    
    df.createOrReplaceTempView('temperatures')
    
    return spark_helper.get_spark().sql("""
        SELECT 
            State,
            MAX(`1`) as `1`, 
            MAX(`2`) as `2`, 
            MAX(`3`) as `3`, 
            MAX(`4`) as `4`, 
            MAX(`5`) as `5`, 
            MAX(`6`) as `6`, 
            MAX(`7`) as `7`, 
            MAX(`8`) as `8`, 
            MAX(`9`) as `9`, 
            MAX(`10`) as `10`, 
            MAX(`11`) as `11`, 
            MAX(`12`) as `12`
        FROM temperatures
        PIVOT(
            MAX(temperature) AS temperature
            FOR month in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        )
        GROUP BY State
    """)

@F.udf(VectorUDT())
def temperatures_to_vector_udf(temperatures_array):
    return Vectors.dense(temperatures_array)

def prepare_features(df):
    return df.withColumn(
        'features',
        temperatures_to_vector_udf(F.array(
            F.col('1'),
            F.col('2'),
            F.col('3'),
            F.col('4'),
            F.col('5'),
            F.col('6'),
            F.col('7'),
            F.col('8'),
            F.col('9'),
            F.col('10'),
            F.col('11'),
            F.col('12'),
        ))
    )

def cluster_temperatures(df):
    kmeans = KMeans().setK(5).setSeed(1)
    model = kmeans.fit(df)
    return model.transform(df)

def project_climate(df):
    
    climate = spark_helper.get_spark().createDataFrame([
                [4 , 3, 'warm'],
                [0 , 1, 'continental'],
                [1 , 2, 'temperate'],
                [2 , 0, 'polar'],
                [3 , 4, 'tropical']
        ],
        ['cluster', 'climate_id', 'climate']
    )
    
    return (
        df
        .join(
            climate,
            on=df['prediction'] == climate['cluster'],
            how='inner'
        )
        .drop('cluster')
    )

def clean_states(df):
    return df.withColumn(
        'State',
        F.expr("""
            IF(
                State LIKE 'Georgia%',
                'Georgia',
                TRIM(State)
            )
        """)
    )

def join_states(df):
    
    states = StatesFile().read()
    
    return (
        df
        .join(
            states,
            on=df['State'] == states['name'],
            how='inner'
        )
    )

def project_schema(df):
    return df.select(
        'state_id',
        F.col('climate_id').cast('int'),
        'climate'
    )

def save_temperatures(df):
    TemperaturesFile().stage(df)

def clean_temperatures():
    return (
        load_temperatures()
        .pipe(filter_us_temperatures)
        .pipe(filter_2012_temperatures)
        .pipe(project_months)
        .pipe(pivot_temperatures)
        .pipe(prepare_features)
        .pipe(cluster_temperatures)
        .pipe(project_climate)
        .pipe(clean_states)
        .pipe(join_states)
        .pipe(project_schema)
        .pipe(save_temperatures)
    )

clean_temperatures()