from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.immigration_file import ImmigrationFile

from datalake.datamodel.files.time_dim_file import TimeDimFile


class ETLDimTimeTest(ETLSparkJobTestBase):

    def setUp(self):
        super().setUp()
        self.spark_job_module = 'etl_spark_jobs.etl_dim_time'
        self.input_file = [
            ImmigrationFile(),
            TimeDimFile()
        ]
        self.output_file = TimeDimFile()
