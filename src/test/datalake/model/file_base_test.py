from etl_test_base import ETLTestBase
from datalake.model.file_base import FileBase
from datalake.utils import test_utils

from datalake.datamodel.files.test_file import TestFile


import os


class FileBaseTests(ETLTestBase):

    def test_datalake_root(self):
        self.assertEqual(
            os.environ['DATALAKE_ROOT'],
            FileBase.get_datalake_root()
        )

        file = FileBase(
            'test_datalake_root_file',
            None,
            FileBase.staging
        )
        self.assertEqual(
            FileBase.get_datalake_root(),
            file._get_datalake_root()
        )

    def test_staging_root(self):
        self.assertEqual(
            os.environ['STAGING_ROOT'],
            FileBase.get_staging_root()
        )

        file = FileBase(
            'test_staging_root_file',
            None,
            FileBase.staging
        )
        self.assertEqual(
            FileBase.get_staging_root(),
            file._get_staging_root()
        )

    # test save

    def test_read_defaults(self):
        (file, data, df) = test_utils.create_file(
            'test_read_defaults', writable=True
        )

        file.save(df)
        expected = data[0][0]
        actual_df = file.read()
        actual = actual_df.collect()[0][0]
        self.assertEqual(expected, actual)

        #assert default mode='append'
        self.assertEqual(1, actual_df.count())
        file.save(df)
        actual_df = file.read()
        self.assertEqual(2, actual_df.count())

    def test_write_defaults(self):
        (file, _, df) = test_utils.create_file('test_write_defaults')

        self.assertRaises(file.FileNotWritableException, file.save, df)

    def test_options(self):
        (file, _, df) = test_utils.create_file(
            'test_options', format='csv', writable=True,
            options={'header': 'true'}
            )
        
        file.save(df)
        self.assertEqual(0, file.read().count()) # the only row is the header

    def test_partitions(self):
        arity = 2
        (file, data, df) = test_utils.create_file(
            'test_partitions', writable=True, arity=arity,
            partitions='col0'
            )
        
        file.save(df)
        expected = data[0]
        actual_df = file.read()
        actual = actual_df.collect()[0].asDict()
        for i in range(arity):
            self.assertEqual(expected[i], actual[f'col{i}'])

    def test_init_is_not_destructive(self):
        name = 'test_init'
        arity = 1
        file, _, _ = test_utils.create_file(
            name, writable=True, arity=arity, empty=True
        )
        
        file.init()

        # an empty file was created
        actual = file.read().count()
        self.assertEqual(0, actual)

        (_, _, df) = test_utils.create_file(
            name, writable=True, arity=arity
        )
        file.save(df)   # save one row in same file
        file.init()     # init same file again
        
        # same file must still have one row in spite of init
        actual = file.read().count()
        self.assertEqual(1, actual)

    def test_save_in_different_area(self):
        name = 'test_save_in_different_area'
        
        (file, _, df) = test_utils.create_file(
            name, FileBase.production, writable=True
        )

        file.save(df, area=FileBase.staging)
        file.read(area=FileBase.staging)
        # exception if fails

    def test_coalesce(self):
        name = 'test_coalesce'
        
        (file, _, df) = test_utils.create_file(
            name, FileBase.curated, writable=True, coalesce=1
        )

        file.save(df)
        # exception if fails

    def test_datalake_save_and_read(self):
        name = 'test_datalake_save_and_read'
        (file, data, df) = test_utils.create_file(
            name, FileBase.curated, writable=True
        )

        file.save(df)

        actual_df = file.read()

        self._assert_read_equals_saved(actual_df, data)

    def test_get_class_from_class_name(self):
        actual = FileBase.get_class_from_class_name('TestFile')
        self.assertEqual(TestFile, actual)

    def test_overwrite_is_idempotent(self):
        """
        Both staging and loading tables must be idempotent.
        Test that idempotency can be achieved on both S3
        and hdfs by using Spark's 'overwrite' mode.
        """
        # Assert that d3 and hdfs are being used for storage.
        # If not, reconfigure DATALAKE_ROOT and STAGING_ROOT env vars.
        assert FileBase.get_datalake_root()[0:2] == 's3'
        assert FileBase.get_staging_root()[0:4] == 'hdfs'

        for area in ['staging', 'production']:
            (file, _, _) = test_utils.create_file(
                'test_overwrite_is_idempotent',
                empty=False,
                mode='overwrite',
                writable=True
            )

            # write the file
            data = 'the same data'
            written_df1 = test_utils.create_df([[data]])
            file.save(written_df1, area=area)

            # write the file again
            # To prevent ```java.io.FileNotFoundException```,
            # recreate the data frame
            written_df2 = test_utils.create_df([[data]])
            file.save(written_df2, area=area)
            read_df2 = file.read(area=area)

            # if mode='overwrite' is idempotent given the same data frame,
            # both reads must have returned the same data frame
            self.assertEqual(written_df1.collect(), read_df2.collect())


    # helpers

    def _assert_read_equals_saved(self, actual_df, data):
        expected = data[0]
        actual = actual_df.collect()[0].asDict()
        arity = len(expected)
        for i in range(arity):
            self.assertEqual(expected[i], actual[f'col{i}'])

if __name__ == '__main__':
	import unittest
	unittest.main()
