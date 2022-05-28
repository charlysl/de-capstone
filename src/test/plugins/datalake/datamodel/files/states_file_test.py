from etl_test_base import ETLTestBase

from datalake.model.datalake import SparkDatalake
from datalake.etl_spark_jobs.etl_clean_states import CleanStatesSparkJob
from datalake.datamodel.files.us_states_and_territories_file import UsStatesAndTerritoriesFile


class StatesFileTest(ETLTestBase):

    def test_clean_state(self):
        file = UsStatesAndTerritoriesFile(self.datalake)
        schema = file.schema
        spark = self.datalake.get_spark()
        arity = len(schema.fieldNames())
        data = [[None for i in range(arity)]]
        df = spark.createDataFrame(data, schema=file.schema)
        file.save(df, force=True)

        CleanStatesSparkJob()

        actual_df = file.read()
