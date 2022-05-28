from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.visitor_dim_file import VisitorDimFile
from datalake.datamodel.files.country_file import CountryFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile


class ETLCleanAirportsTest(ETLSparkJobTestBase):

    def setUp(self):
        super().setUp()

        self.spark_job_module = 'etl_spark_jobs.etl_dim_foreign_visitor'
        self.input_file = [ImmigrationFile(), VisitorDimFile(), CountryFile(), StatesFile(), TemperaturesFile()]
        self.output_file = [VisitorDimFile()]
