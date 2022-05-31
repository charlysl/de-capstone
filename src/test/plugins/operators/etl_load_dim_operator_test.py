"""
Must call get_spark() here, otherwise get exception when
creating a data frame.
"""
import findspark
findspark.init()
from datalake.utils import spark_helper
spark = spark_helper.get_spark()

from operators.etl_load_dim_operator import ETLLoadDimOperator
from operators.etl_operator_test_base import ETLOperatorTestBase
from datalake.datamodel.files.test_file import TestFile
from datalake.utils import test_utils


class ETLLoadDimOperatorTest(ETLOperatorTestBase):

    def test_etl_load_dim(self):
        # datalake_root was set as Airflow variable, only passed to job,
        # so have to set it explicitly
        file = TestFile(datalake_root=self.get_datalake_root())

        value = 'test_data'
        df = test_utils.create_df([[value]])
        file.save(df, area=TestFile.staging)

        self._test_task(
            ETLLoadDimOperator(
                file='TestFile',
                dag=self.dag
            )
        )

        expected = value
        actual = file.read(area=TestFile.production).collect()[0][0]
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    import unittest
    unittest.main()
