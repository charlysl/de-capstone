from etl_test_base import ETLTestBase

from datalake.utils import spark_helper

import importlib
import sys


class ETLSparkJobTestBase(ETLTestBase):

    def setUp(self):
        self.date = '2015-01-01'
        # to achive an empty run
        sys.argv=['dummy', self.date]



    def test_empty(self):
        if not self.__dict__.get('spark_job_module'):
            # This check is required because, otherwise,
            # unittest would run this test twice, once
            # for this class's subclass, and once for this
            # base class itself. This check prevents the latter,
            # because in that base the fields set by the sub test
            # would be missing, for instance 'spark_job_module'.
            return

        self._save_empty_files()

        #see https://stackoverflow.com/questions/64916281/importing-modules-dynamically-in-python-3-x
        importlib.import_module(self.spark_job_module)

        self.output_file.read()
        # would throw an exception if output file was missing


    # helpers

    def _save_empty_files(self):
        for file in self._input_file_to_list():
            self._save_empty_file(file)

    def _input_file_to_list(self):
        files = self.input_file
        return files if type(files) == list else [files]

    def _save_empty_file(self, file, **kwargs):
        self._save_file(file, [], **kwargs)

    def _save_file(self, file, data, **kwargs):
        schema = file.schema
        spark = spark_helper.get_spark()
        arity = len(schema.fieldNames())
        #data = [[None for i in range(arity)]]
        df = spark.createDataFrame(data, schema=file.schema)
        file.save(df, force=True, **kwargs)
        return file