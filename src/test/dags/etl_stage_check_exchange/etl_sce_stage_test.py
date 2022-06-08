from dags.etl_stage_check_exchange.etl_sce_test_base import ETLSceTestBase


class ETLSceStageTests(ETLSceTestBase):

    def test_etl_spark_operator_application_from_name(self):
        self._test_task(
            self.sce_tasks.stage(
                name='test',
                dag=self.dag
            )
        )

    def test_etl_spark_operator_application(self):
        self._test_task(
            self.sce_tasks.stage(
                name='test_etl_spark_operator_application',
                application='test',
                dag=self.dag
            )
        )

    def test_etl_spark_operator_read_file(self):
        self._test_task(
            self.sce_tasks.stage(
                name='spark_operator_read_file',
                dag=self.dag
            )
        )

    def test_etl_spark_operator_date(self):
        self._test_task(
            self.sce_tasks.stage(
                name='test_etl_spark_operator_date',
                application='test',
                date='2016-01-01',
                dag=self.dag
            )
        )

if __name__ == '__main__':
    import unittest
    unittest.main()