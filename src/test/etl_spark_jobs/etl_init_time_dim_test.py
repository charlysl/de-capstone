from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.time_dim_file import TimeDimFile

class ETLInitTimeDimTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_init_time_dim'
        self.input_file = []
        self.output_file = TimeDimFile()
