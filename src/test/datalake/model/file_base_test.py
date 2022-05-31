from etl_test_base import ETLTestBase
from datalake.model.file_base import FileBase
from datalake.utils import test_utils

import os


class FileBaseTests(ETLTestBase):

    # test save

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
            file.datalake_root
        )


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

    def test_hdfs_save(self):
        pass
