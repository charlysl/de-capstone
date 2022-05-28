from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.sas_file import SasFile
from datalake.datamodel.files.airports_file import AirportsFile


class ETLCleanImmigrationTest(ETLSparkJobTestBase):

    def setUp(self):
        super().setUp()

        self.spark_job_module = 'etl_spark_jobs.etl_clean_immigration'
        self.input_file = SasFile(self.date)
        self.output_file = AirportsFile
