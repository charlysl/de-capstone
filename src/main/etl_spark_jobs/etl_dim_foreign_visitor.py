import pyspark.sql.functions as F
import pyspark.sql.types as T

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.age import Age
from datalake.datamodel.stay import Stay

from datalake.datamodel.files.visitor_dim_file import VisitorDimFile

from datalake.datamodel.files.country_file import CountryFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile

from datalake.utils import spark_helper


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


def load_immigration():
    return ImmigrationFile().read()

def visitor_dim_nk(df):
    return (
        df.select(VisitorDimFile().get_nk())
        .drop_duplicates()
    )

def join_immigration_with_visitor_dim(df, visitor_dim):
    return (
        df
        .join(
            visitor_dim, on=VisitorDimFile().on_nk(df, visitor_dim), how='leftanti'
        )
    )

def fill_sk(df):
    return df.withColumn('visitor_id', F.expr(VisitorDimFile().gen_sk_expr()))

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

def union_visitor_dim(df, visitor_dim):
    return df.select(visitor_dim.columns).union(visitor_dim)

def save_visitor_dim(df):
    VisitorDimFile().stage(df)

def fill_missing_visitor(date, visitor_dim):
    
    country = CountryFile().read()
    state = StatesFile().read()
    temperature = TemperaturesFile().read()

    return (
        load_immigration()
        .pipe(spark_helper.filter_one_month, date)
        .pipe(visitor_dim_nk)
        .pipe(join_immigration_with_visitor_dim, visitor_dim)
        .pipe(fill_sk)
        .pipe(fill_country, country, 'citizenship_id', 'citizenship')
        .pipe(fill_country, country, 'residence_id', 'residence')
        .pipe(fill_age)
        .pipe(fill_gender)
        .pipe(fill_visa)
        .pipe(fill_state, state)
        .pipe(fill_climate, temperature)
        .pipe(fill_stay)
        .pipe(project_schema)
        .pipe(union_visitor_dim, visitor_dim)
        .pipe(save_visitor_dim)
    )


fill_missing_visitor(
    spark_helper.get_date(),
    VisitorDimFile().read()
)
