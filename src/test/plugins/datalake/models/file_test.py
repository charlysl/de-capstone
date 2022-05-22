from etl_test_base import ETLTestBase

from datalake.model.datalake import SparkDatalake
from datalake.model.file import DatalakeFile

import shutil

class FileTests(ETLTestBase):

    def setUp(self):
        self.datalake = SparkDatalake('/tmp/datalake')

    def tearDown(self):
        # It is by design that I don't use a variable to refer to the datalake
        # directory, because it could be accidentaly overwritten, risking
        # deletion of valuable files.
        if shutil.os.path.exists('/tmp/datalake'):
            shutil.rmtree('/tmp/datalake')

    # test save

    def test_read_defaults(self):
        (file, data, df) = self._create_file(
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
        (file, _, df) = self._create_file('test_write_defaults')

        self.assertRaises(file.FileNotWritableException, file.save, df)

    def test_options(self):
        (file, _, df) = self._create_file(
            'test_options', format='csv', writable=True,
            options={'header': 'true'}
            )
        
        file.save(df)
        self.assertEqual(0, file.read().count()) # the only row is the header

    def test_partitions(self):
        arity = 2
        (file, data, df) = self._create_file(
            'test_partitions', writable=True, arity=arity
            )
        
        file.save(df, partitions='col0')
        expected = data[0]
        actual_df = file.read()
        actual = actual_df.collect()[0].asDict()
        for i in range(arity):
            self.assertEqual(expected[i], actual[f'col{i}'])


    # test utils

    def _create_file(self, test_name, 
                    area=DatalakeFile.staging, arity=1, **kwargs):
        data = [[f'{test_name}_data{i}' for i in range(arity)]]
        df = self.create_df(data)

        name = f'{test_name}_file'
        schema = df.schema
        datalake = self.datalake
        
        return (
            DatalakeFile(name, schema, datalake, area, **kwargs),
            data,
            df
        )


