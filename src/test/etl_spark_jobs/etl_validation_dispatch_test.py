import json

from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase
from etl_spark_jobs.etl_validation import ETLValidationDispatch, ETLValidation

from datalake.datamodel.files.states_file import StatesFile
# TODO it would be better to either mock or create dummy file class


class ETLValidationDispatchTest(ETLSparkJobTestBase):

    test_table = 'datalake.datamodel.files.states_file.StatesFile'

    def test_dispatch_invalid_one_table_no_column(self):

        self._save_empty_file(StatesFile())

        argv = self._argv('check_not_empty')
        
        self.assertRaises(
            ETLValidation.IsEmptyException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_one_column(self):

        self._save_nulls_file(StatesFile())

        argv = self._argv('check_no_nulls', column='state_id')
        
        self.assertRaises(
            ETLValidation.HasNullsException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_two_columns(self):

        self._save_file_with_value(StatesFile(), 2, 1)

        argv = self._argv(
            'check_no_duplicates',
            column=['state_id', 'name']
        )
        
        self.assertRaises(
            ETLValidation.HasDuplicatesException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_two_tables_one_column(self):

        self._save_file_with_value(StatesFile(), 1, 1)
        self._save_file_with_value(StatesFile(), 1, 2, area=StatesFile.staging)

        table = ETLValidationDispatchTest.test_table
        argv = self._argv(
            'check_referential_integrity',
            area=['curated', 'staging'],
            column = ['state_id'],
            table=[table, table]
        )
        
        self.assertRaises(
            ETLValidation.ReferentialIntegrityException,
            ETLValidationDispatch(argv).dispatch,
        )

    # do one with date on immigration

    # helpers

    def _argv(
            self,
            check,
            column=None,
            area=None,
            table=test_table
        ):
        args = {}
        args['check'] = check
        args['table'] = table
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
                        **kwargs)


if __name__ == '__main__':
    import unittest
    unittest.main()