from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.raw_demographics_file import RawDemographicsFile
from datalake.datamodel.files.airports_file import AirportsFile


class ETLCleanDemographicsTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_demographics'
        self.input_file_class = RawDemographicsFile()
        self.output_file_class = AirportsFile()
