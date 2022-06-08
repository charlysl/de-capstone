from dags.etl_stage_check_exchange.etl_sce_test_base import ETLSceTestBase

from datalake.datamodel.files.test_file import TestFile
from datalake.utils import test_utils

class ETLSceCheckTests(ETLSceTestBase):
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
            self.sce_tasks.check(
                name='test_check_not_empty', 
                check='check_not_empty',
                table='TestFile',
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
            self.sce_tasks.check(
                name='test_check_not_empty', 
                check='check_not_empty',
                table='TestFile',
                area='staging',
                dag=self.dag
            )
        )

        self._test_task(task)
        #pass


if __name__ == '__main__':
    import unittest
    unittest.main()