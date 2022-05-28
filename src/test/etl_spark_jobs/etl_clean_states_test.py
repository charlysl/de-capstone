from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.raw_states_file import RawStatesFile
from datalake.datamodel.files.states_file import StatesFile


class ETLCleanStatesTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_states'
        self.input_file = RawStatesFile()
        self.output_file = StatesFile()
