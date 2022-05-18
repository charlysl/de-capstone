import sys
import json

import pyspark.sql.functions as F

from etl import SparkETL


class ETLValidation():

    class IsEmptyException(ValueError): pass
    class HasNullsException(ValueError): pass
    class HasDuplicatesException(ValueError): pass
    class ReferentialIntegrityException(ValueError): pass

    def __init__(self, df):
        self.df = df

    def check_not_empty(self):
        if ETLValidation._df_is_empty(self.df):
            raise self.IsEmptyException

    def check_no_nulls(self):
        df_with_nulls = self.df.where(self._sql_expr__has_nulls())
        if not ETLValidation._df_is_empty(df_with_nulls):
            raise self.HasNullsException
 
    def check_no_duplicates(self):
        count = self.df.count()
        no_dup_count = self.df.drop_duplicates().count()
        if count != no_dup_count:
            msg = f"check_no_duplicates {count} != {no_dup_count}"
            raise self.HasDuplicatesException(msg)

    def check_referential_integrity(self, df2):
        on = self.df[self.df.columns[0]] == df2[df2.columns[0]]
        leftanti = self.df.join(df2, on=on, how='leftanti')
        if not ETLValidation._df_is_empty(leftanti):
            raise self.ReferentialIntegrityException

    def _sql_expr__has_nulls(self):
        sql = []
        for col in self.df.columns:
            sql.append(f"{col} is null")
        return " or ".join(sql)

    @staticmethod
    def _df_is_empty(df):
        return len(df.head(1)) == 0

    def _handle_failure(self, msg):
        raise ValueError(f"validation failed: {msg}")


class ETLValidationDispatch():

    def __init__(self, argv):
        self.argv = argv

    def dispatch(self):
        """
        Description: Extract arguments from argv, create views 
                     and invoke check.
        """
        kwargs = self._unpack_application_kwargs()

        dfs = self._create_views(
            self._to_array(kwargs['table']),
            self._to_array(kwargs['kind']),
            kwargs['column']
        )

        self._invoke_check(kwargs['check'], dfs)

    def _invoke_check(self, check, dfs):
        # call a python method by name on an instance
        # see https://stackoverflow.com/questions/3521715/call-a-python-method-by-nam
        getattr(ETLValidation(dfs[0]), check)(*dfs[1:])

    def _create_views(self, tables, kinds, columns):
        return [(
            SparkETL()
            .read_table(tables[i], kinds[i])
            .select(columns)
        ) for i in range(len(tables))]

    def _unpack_application_kwargs(self):
        # argv[1] is the script name, skip
        return json.loads(self.argv[1])

    def _to_array(self, kwarg):
        return [kwarg] if type(kwarg) == str else kwarg


if __name__ == '__main__':
    ETLValidationDispatch(sys.argv).dispatch()
