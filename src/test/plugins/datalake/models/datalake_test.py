import unittest

from etl_test_base import ETLTestBase
from datalake import SparkDatalake


class DatalakeTest(ETLTestBase):

    def setUp(self):
        super().setUp()
        self.datalake = SparkDatalake(self.get_root())

    # test save

    def test_save(self):
        dl = self.datalake
        for area in [dl.eda, dl.raw, dl.curated, dl.staging, dl.production]:
            name = 'a_table'
            expected = [['something']]

            df = self.create_df(expected)
            area.save(df, name)
            actual = area.read(name)

            self.assertEqual(expected[0][0], actual.collect()[0][0])

    def test_save_staging_overwrites(self):
        name = 'a_table'
        overwritten = [['something']]
        expected = [['something_else']]

        for data in [overwritten, expected]:
            df = self.create_df(data)
            self.datalake.staging.save(df, name)
        actual = self.datalake.staging.read(name)

        self.assertEqual(expected[0][0], actual.collect()[0][0])


if __name__ == '__main__':
    unittest.main()