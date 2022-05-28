from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from datalake.datamodel.files.i94_data_dictionary_file import I94DataDictionaryFile
from datalake.datamodel.files.country_file import CountryFile


class ETLCleanCountryTest(ETLSparkJobTestBase):

    def setUp(self):
        self.spark_job_module = 'etl_spark_jobs.etl_clean_country'
        self.input_file_class = I94DataDictionaryFile()
        self.output_file_class = CountryFile()
