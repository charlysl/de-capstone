from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.airports_file import AirportsFile
from datalake.datamodel.files.ports_file import PortsFile
from datalake.datamodel.files.ports_to_airports_file import PortsToAirportsFile


class ETLCleanAirportsTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_ports_to_airports'
        self.input_file = [PortsFile(), AirportsFile()]
        self.output_file = PortsToAirportsFile()
