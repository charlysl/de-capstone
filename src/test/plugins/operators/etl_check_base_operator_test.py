from operators.etl_operator_test_base import ETLOperatorTestBase
from operators.etl_check_base_operator import ETLCheckBaseOperator
from datalake.datamodel.files.test_file import TestFile
from datalake.utils import test_utils
from etl_spark_jobs.etl_validation import ETLValidation

class ETLCheckBaseOperatorTests(ETLOperatorTestBase):
    """
    These are integration tests. As such, there are some prerequisites.

    ### Prerequisites:

    For these tests to work you must first ensure that:
    - there is a link to ../../../../main/etl_spark_jobs/etl_validation.py
      in the directorysrc/test/plugins/operators/etl_spark_jobs.
      To create it, navigate to that directory and execute:
      ```
      ln - s ../../../../main/etl_spark_jobs/etl_validation.py
      ```
    """

    def test_check_not_empty_when_empty(self):

        # stage an empty dataset
        empty_df = test_utils.create_df([])
        TestFile().stage(empty_df)

        task = (
            ETLCheckBaseOperator(
                name='test_check_not_empty', 
                check='check_not_empty',
                table='datalake.datamodel.files.test_file.TestFile',
                area='staging',
                dag=self.dag
            )
        )

        self.assertRaises(
            Exception,
            self._test_task,
            task
        )

    def test_check_not_empty_when_not_empty(self):

        # stage an non-empty dataset
        not_empty_df = test_utils.create_df([['not empty!']])
        TestFile().stage(not_empty_df)

        task = (
            ETLCheckBaseOperator(
                name='test_check_not_empty', 
                check='check_not_empty',
                table='datalake.datamodel.files.test_file.TestFile',
                area='staging',
                dag=self.dag
            )
        )

        self._test_task(task)
        #pass
        

if __name__ == '__main__':
    import unittest
    unittest.main()
