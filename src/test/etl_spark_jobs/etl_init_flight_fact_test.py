from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.flight_fact_file import FlightFactFile

class ETLInitTimeDimTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_init_flight_fact'
        self.input_file = []
        self.output_file = FlightFactFile()
