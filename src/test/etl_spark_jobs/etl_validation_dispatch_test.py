import json

from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase
from etl_spark_jobs.etl_validation import ETLValidationDispatch, ETLValidation
from datalake.datamodel.files.test_file import TestFile
from datalake.datamodel.files.test_file2 import TestFile2

# TODO it would be better to either mock or create dummy file class


class ETLValidationDispatchTest(ETLSparkJobTestBase):

    def tearDown(self):
        pass

    def test_dispatch_invalid_one_table_no_column(self):

        self.test_table = 'TestFile'

        self._save_empty_file(TestFile(), area='staging')

        argv = self._argv('check_not_empty')
        
        self.assertRaises(
            ETLValidation.IsEmptyException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_one_column(self):

        self.test_table = 'TestFile'

        self._save_nulls_file(TestFile())

        argv = self._argv('check_no_nulls', column='col0')
        
        self.assertRaises(
            ETLValidation.HasNullsException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_two_columns(self):

        self.test_table = 'TestFile2'

        self._save_file_with_value(TestFile2(), 2, 1)

        argv = self._argv(
            'check_no_duplicates',
            column=['col0', 'col1']
        )
        
        self.assertRaises(
            ETLValidation.HasDuplicatesException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_two_tables_one_column(self):

        self._save_file_with_value(TestFile(), 1, 1)
        self._save_file_with_value(TestFile2(), 1, 2)

        argv = self._argv(
            'check_referential_integrity',
            column = ['col0'],
            table=['TestFile', 'TestFile2']
        )
        
        self.assertRaises(
            ETLValidation.ReferentialIntegrityException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_two_tables_two_columns(self):

        self._save_file_with_value(TestFile(), 1, 1)
        self._save_file_with_value(TestFile2(), 1, 2)

        argv = self._argv(
            'check_referential_integrity',
            column = ['col1', 'col0'],
            table=['TestFile2', 'TestFile']
        )
        print(argv)
        
        self.assertRaises(
            ETLValidation.ReferentialIntegrityException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_check_referential_integrity_integration(self):
        argv = ['dummy', '{"check": "check_referential_integrity", "table": ["ImmigrationFile", "CountryFile"], "area": null, "column": ["residence_id", "country_id"], "date": "2016-01-01"}']
        ETLValidationDispatch(argv).dispatch()

    # helpers

    def _argv(
            self,
            check,
            column=None,
            area=None,
            table=None
        ):
        args = {}
        args['check'] = check
        args['table'] = self.test_table if not table else table
        args['column'] = column
        args['area'] = area
        return ['some_path', json.dumps(args)]

    def _save_nulls_file(self, file):
        self._save_file_with_value(file, 1, None)

    def _save_file_with_value(self, file, nrows, value, **kwargs):
        arity = len(file.schema)
        self._save_file(file,
                        [
                            [value for _ in range(arity)]
                            for i in range(nrows)
                        ],
                        area='staging', # validation always in staging
                        **kwargs)


if __name__ == '__main__':
    import unittest
    unittest.main()