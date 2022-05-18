import json

from etl_test_base import ETLTestBase
from etl_validation import ETLValidationDispatch, ETLValidation


class TestETLValidationDispatch(ETLTestBase):

    def test_dispatch_invalid_one_table_no_column(self):
        table = 'is_empty'

        self.save_df(
            self.create_df([]),
            table
        )

        args = {}
        args['check'] = 'check_not_empty'
        args['table'] = table
        args['kind'] = 'test'
        args['column'] = None
        argv = ['some_path', json.dumps(args)]
        
        self.assertRaises(
            ETLValidation.IsEmptyException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_one_column(self):
        table = 'has_nulls'

        self.save_df(
            self.create_df([[None]]),
            table
        )

        args = {}
        args['check'] = 'check_no_nulls'
        args['table'] = table
        args['kind'] = 'test'
        args['column'] = 'col0'
        argv = ['some_path', json.dumps(args)]
        
        self.assertRaises(
            ETLValidation.HasNullsException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_one_table_two_columns(self):
        table = 'has_duplicates'

        row = ['something', 'something_else']
        self.save_df(
            self.create_df([row, row]),
            table
        )

        args = {}
        args['check'] = 'check_no_duplicates'
        args['table'] = table
        args['kind'] = 'test'
        args['column'] = ['col0', 'col1']
        argv = ['some_path', json.dumps(args)]
        
        self.assertRaises(
            ETLValidation.HasDuplicatesException,
            ETLValidationDispatch(argv).dispatch,
        )

    def test_dispatch_invalid_two_tables_one_column(self):
        tables = ['table1', 'table2']

        self.save_df(
            self.create_df([['something']]),
            tables[0]
        )
        self.save_df(
            self.create_df([['something_else']]),
            tables[1]
        )

        args = {}
        args['check'] = 'check_referential_integrity'
        args['table'] = tables
        args['kind'] = ['test', 'test']
        args['column'] = ['col0']
        argv = ['some_path', json.dumps(args)]
        
        self.assertRaises(
            ETLValidation.ReferentialIntegrityException,
            ETLValidationDispatch(argv).dispatch,
        )

    # do one with date on immigration

