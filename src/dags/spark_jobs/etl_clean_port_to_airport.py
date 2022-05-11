from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, NGram, HashingTF, MinHashLSH, StopWordsRemover, SQLTransformer

import pyspark.sql.functions as F

from etl import SparkETL
from stopwords import Stopwords

etl = SparkETL()

spark = etl.get_spark()

airports = etl.read_clean_table('airport')

ports = etl.read_clean_table('port')

def db(df):
    return (
        df
        .select(
            'airport_id',
            'state_id',
            'international',
            'type_id',
            F.lower(F.concat(
                F.col('city'), F.lit(' '), F.col('name')
            ))
            .alias('text')
        )
        .where(~F.col('text').isNull())
    )

def query(df):
    return df.select(
        'port_id',
        F.col('state_id').alias('port_state_id'),
        F.lower(F.col('name')).alias('text'),
    )

stopwords = (
    Stopwords([
        airports.pipe(db),
        ports.pipe(query)
    ])
    .stopwords(50)
)

def set_locale():
    # see https://stackoverflow.com/questions/55246080/pyspark-stopwordsremover-parameter-locale-given-invalid-value
    locale = spark.sparkContext._jvm.java.util.Locale
    locale.setDefault(locale.forLanguageTag("en-US"))

def create_model(df):
    """
    df is the db
    """
    set_locale()

    # to prevent https://stackoverflow.com/questions/55628049/string-matching-using-ml-pipeline-is-throwing-error-failed-to-execute-user-defin
    # see https://stackoverflow.com/questions/53371039/apache-spark-ml-pipeline-filter-empty-rows-in-dataset
    emptyRemover = SQLTransformer().setStatement(
          "SELECT * FROM __THIS__ WHERE size(tokens) > 0"
    )
    
    return (
        Pipeline(stages=[
            RegexTokenizer(
                pattern=Stopwords.token_separator, inputCol="text", outputCol="tokens0", minTokenLength=3
            ),
            #NGram(n=1, inputCol="tokens", outputCol="ngrams"),
            #HashingTF(inputCol="ngrams", outputCol="vectors"),
            StopWordsRemover(
                stopWords=stopwords,
                inputCol="tokens0",
                outputCol="tokens",
            ),
            emptyRemover,
            HashingTF(inputCol="tokens", outputCol="vectors"),
            MinHashLSH(inputCol="vectors", outputCol="lsh")
        ])
        .fit(df)
    )

airport_db = db(airports)

model = create_model(airport_db)

db_hashed = model.transform(airport_db)

query_hashed = model.transform(ports.pipe(query))

distances = model.stages[-1].approxSimilarityJoin(db_hashed, query_hashed, 1.0)

def project_distances(df):
    return (
        df
        .select(
            F.col('datasetA.airport_id').alias('airport_id'),
            F.col('datasetA.text').alias('airport_text'),
            F.col('datasetA.state_id').alias('state_id'),
            F.col('datasetA.international').alias('international'),
            F.col('datasetA.type_id').alias('type_id'),
            F.col('datasetB.port_id').alias('port_id'),
            F.col('datasetB.port_state_id').alias('port_state_id'),
            F.col('datasetB.text').alias('port_text'),
            'distCol'
        )
    )

def same_state(df):
    return df.where(F.col('port_state_id') == F.col('state_id'))

def sort_distances(df):

    df.createOrReplaceTempView('distances')

    spark = df.sql_ctx.sparkSession

    return spark.sql("""
        SELECT *
        FROM distances
        ORDER BY
            port_state_id,
            port_id,
            international DESC,
            type_id DESC,
            distCol DESC
    """)

    """
    # Can't use Spark api, causes exception:
    # py4j.Py4JException: Method desc([class org.apache.spark.sql.Column]) does not exist

    return df.sort(
        F.col('port_state_id'),
        F.col('port_id'),
        F.desc(F.col('international')),
        F.desc(F.col('type_id')),
        F.desc(F.col('distCol')),
    )
    """

def best_distance(df):
    return (
        df
        .groupby('port_id')
        .agg(
            F.first('port_state_id'),
            F.first('port_text'),
            F.first('airport_id').alias('airport_id'),
            F.first('airport_text'),
            F.first('distCol').alias('distance')
        )
    )

def project_schema(df):
    return df.select('port_id', 'airport_id')

def port_to_airport(df):
    return (
        df
        .pipe(project_distances)
        .pipe(same_state)
        .pipe(sort_distances)
        .pipe(best_distance)
        .pipe(project_schema)
    )

etl.save_clean_table(port_to_airport(distances).coalesce(1), 'port_to_airport')