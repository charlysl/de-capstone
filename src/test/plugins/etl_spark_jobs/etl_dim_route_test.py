from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.route_dim_file import RouteDimFile

from datalake.datamodel.files.airports_file import AirportsFile
from datalake.datamodel.files.ports_to_airports_file import PortsToAirportsFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.demographics_file import DemographicsFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile


class ETLDimRouteTest(ETLSparkJobTestBase):

    def setUp(self):
        super().setUp()
        self.spark_job_module = 'etl_spark_jobs.etl_dim_route'
        self.input_file = [
            ImmigrationFile(),
            RouteDimFile(),
            AirportsFile(),
            PortsToAirportsFile(),
            StatesFile(),
            DemographicsFile(),
            TemperaturesFile()
        ]
        self.output_file = RouteDimFile()
