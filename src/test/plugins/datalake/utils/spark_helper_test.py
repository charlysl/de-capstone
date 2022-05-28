from etl_test_base import ETLTestBase

from datalake.utils import spark_helper


class SparkHelperTest(ETLTestBase):
    def test_get_spark(self):
        for _ in range(2):
            self.assertIsNotNone(spark_helper.get_spark())