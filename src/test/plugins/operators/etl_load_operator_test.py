"""
Must call get_spark() here, otherwise get exception when
creating a data frame.
"""
import findspark
findspark.init()
from datalake.utils import spark_helper
spark = spark_helper.get_spark()

from operators.etl_load_operator import ETLLoadOperator
from operators.etl_operator_test_base import ETLOperatorTestBase
from datalake.datamodel.files.test_file import TestFile
from datalake.utils import test_utils


class ETLLoadOperatorTest(ETLOperatorTestBase):

    def test_etl_load(self):
        # datalake_root was set as Airflow variable, only passed to job,
        # so have to set it explicitly
        file = TestFile(
            mode='overwrite',
        )

        value = 'test_data'
        df = test_utils.create_df([[value]])
        file.area = TestFile.curated
        file.stage(df)

        self._test_task(
            ETLLoadOperator(
                name='load_test_file',
                file='TestFile',
                dag=self.dag
            )
        )

        expected = value
        actual = file.read().collect()[0][0]
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    import unittest
    unittest.main()
