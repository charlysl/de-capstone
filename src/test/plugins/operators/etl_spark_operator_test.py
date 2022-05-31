from operators.etl_spark_operator import ETLSparkOperator
from operators.etl_operator_test_base import ETLOperatorTestBase


class ETLSparkOperatorTest(ETLOperatorTestBase):

    def test_etl_spark_operator_application_from_name(self):
        self._test_task(
            ETLSparkOperator(
                name='test', 
                dag=self.dag
            )
        )

    def test_etl_spark_operator_application(self):
        self._test_task(
            ETLSparkOperator(
                name='test_etl_spark_operator_application',
                application='test',
                dag=self.dag
            )
        )

    def test_etl_spark_operator_read_file(self):

        self._mock_datalake_root_variable()

        self._test_task(
            ETLSparkOperator(
                name='spark_operator_read_file',
                dag=self.dag
            )
        )