from dags.etl_stage_check_exchange.etl_sce_test_base import ETLSceTestBase

from datalake.datamodel.files.test_file import TestFile
from datalake.utils import test_utils


class ETLSceExchangeTests(ETLSceTestBase):

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
            self.sce_tasks.exchange(
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