import sys

from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase
from datalake.datamodel.files.test_file import TestFile
from datalake.datamodel.files.test_staging_file import TestStagingFile
from datalake.utils import test_utils

from etl_spark_jobs import etl_load


class ETLLoadTest(ETLSparkJobTestBase):

    def test_load_to_curated(self):
        file = TestFile()
        assert file.area == file.curated
        file.writable =True

        df = test_utils.create_df([])
        file.save(df, area=TestFile.staging)

        etl_load.load('TestFile')

        file.read()
        #pass

    def test_load_to_staging(self):
        file = TestStagingFile()
        assert file.area == file.staging
        file.writable =True

        df = test_utils.create_df([])
        file.save(df, area=TestFile.staging)

        etl_load.load('TestStagingFile')

        file.read()
        #pass

if __name__ == '__main__':
    import unittest
    unittest.main()


