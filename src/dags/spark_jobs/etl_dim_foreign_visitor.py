import pyspark.sql.functions as F
import pyspark.sql.types as T

from etl import SparkETL
from age import Age
from stay import Stay

"""
Possible (but unlikely) **combinatorial explosion** for foreign_visitor_dim:
```
            'citizenship_id', 200 values
            'residence_id',   200 values
            'age_id',         5 values
            'gender_id',      3 values
            'visa_id',        3 values
            'address_id',     50 values
            'stay_id'         4 values
```

print('potentially num_rows ~= %e' % (200 * 200 * 5 * 3 * 3 * 50 * 4))
"""

etl = SparkETL()
spark = etl.get_spark()

immigration = etl.read_clean_table('immigration')

visitor_dim = etl.read_dim_table('foreign_visitor_dim')

def visitor_dim_nk(df):
    return (
        df.select(
            'citizenship_id',
            'residence_id',
            'age_id',
            'gender_id',
            'visa_id',
            'address_id',
            'stay_id'
        )
        .drop_duplicates()
    )

def join_immigration_with_visitor_dim(df):
    return (
        df
        .join(
            visitor_dim,
            on=(
                (df['citizenship_id'] == visitor_dim['citizenship_id'])
                & (df['residence_id'] == visitor_dim['residence_id'])
                & (df['age_id'] == visitor_dim['age_id'])
                & (df['gender_id'] == visitor_dim['gender_id'])
                & (df['visa_id'] == visitor_dim['visa_id'])
                & (df['address_id'] == visitor_dim['address_id'])
                & (df['stay_id'] == visitor_dim['stay_id'])
            ),
            how='leftanti'
        )
    )

def fill_pk(df):
    return df.withColumn('visitor_id', F.monotonically_increasing_id())

def fill_country(df, country, left_on, alias):
    return (
        df
        .join(country, on=df[left_on] == country['country_id'], how='left')
        .withColumnRenamed('country', alias)
        .drop('country_id')
    )

@F.udf(T.StringType())
def fill_age_udf(age_id):
    return Age.descriptions[age_id]

def fill_age(df):
    return df.withColumn('age', fill_age_udf(F.col('age_id')))

def fill_gender(df):
    return df.withColumn(
        'gender',
        F.expr("""
            CASE gender_id
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE gender_id
            END
        """)
    )

def fill_visa(df):
    return df.withColumn(
        'visa',
        F.expr("""
            CASE visa_id
                WHEN 1 THEN 'Business'
                WHEN 2 THEN 'Pleasure'
                WHEN 3 THEN 'Student'
                ELSE visa_id
            END
        """)
    )

def fill_state(df, state):
    return (
        df
        .join(
            state,
            on=df['address_id'] == state['state_id'],
            how='left'
        )
        .drop('state_id')
        .withColumnRenamed('name', 'address_state')
        .withColumnRenamed('type_id', 'address_type_id')
        .withColumnRenamed('type', 'address_type')
    )

def fill_climate(df, temperature):
    return (
        df
        .join(
            temperature,
            on=df['address_id'] == temperature['state_id'],
            how='left'
        )
        .drop('state_id')
        .withColumnRenamed('climate_id', 'address_climate_id')
        .withColumnRenamed('climate', 'address_climate')
    )

@F.udf(T.StringType())
def fill_stay_udf(stay_id):
    return Stay.descriptions[stay_id]

def fill_stay(df):
    return df.withColumn('stay', fill_stay_udf(F.col('stay_id')))

def project_schema(df):
    return df.select(
        'visitor_id',
        'citizenship_id',
        'residence_id',
        'age_id',
        'gender_id',
        'visa_id',
        'address_id',
        'stay_id',
        'citizenship',
        'residence',
        'age',
        'gender',
        'visa',
        'address_state',
        'address_type_id',
        'address_type',
        'address_climate_id',
        'address_climate',
        'stay'
    )

def missing_visitor(df):
    
    country = etl.read_clean_table('country')
    state = etl.read_clean_table('state')
    temperature = etl.read_clean_table('temperature')

    return (
        immigration
        .pipe(visitor_dim_nk)
        .pipe(join_immigration_with_visitor_dim)
        .pipe(fill_pk)
        .pipe(fill_country, country, 'citizenship_id', 'citizenship')
        .pipe(fill_country, country, 'residence_id', 'residence')
        .pipe(fill_age)
        .pipe(fill_gender)
        .pipe(fill_visa)
        .pipe(fill_state, state)
        .pipe(fill_climate, temperature)
        .pipe(fill_stay)
        .pipe(project_schema)
    )

etl.save_dim_table(
    immigration.pipe(missing_visitor),
    'foreign_visitor_dim'
)