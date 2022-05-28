from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.route_dim_file import RouteDimFile

class ETLInitRouteDimTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_init_route_dim'
        self.input_file = []
        self.output_file = RouteDimFile()
