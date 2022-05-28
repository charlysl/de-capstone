from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.raw_temperatures_file import RawTemperaturesFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile
from datalake.datamodel.files.states_file import StatesFile

class ETLCleanTemperatuersTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_temperatures'
        self.input_file = [RawTemperaturesFile(), StatesFile()]
        self.output_file = TemperaturesFile()
