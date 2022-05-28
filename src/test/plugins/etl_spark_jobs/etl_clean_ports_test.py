from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.i94_data_dictionary_file import I94DataDictionaryFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.ports_file import PortsFile

class ETLCleanAirportsTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_ports'
        self.input_file = [I94DataDictionaryFile(), StatesFile()]
        self.output_file = PortsFile()
