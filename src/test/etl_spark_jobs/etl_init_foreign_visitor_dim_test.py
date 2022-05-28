from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.visitor_dim_file import VisitorDimFile


class ETLInitForeignVisitorDimTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_init_foreign_visitor_dim'
        self.input_file = []
        self.output_file = VisitorDimFile()
        
