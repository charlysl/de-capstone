from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.raw_airports_file import RawAirportsFile
from datalake.datamodel.files.airports_file import AirportsFile


class ETLCleanAirportsTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_airports'
        self.input_file = RawAirportsFile()
        self.output_file = AirportsFile()
