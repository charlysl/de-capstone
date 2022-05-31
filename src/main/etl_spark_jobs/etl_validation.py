import sys
import json
import importlib

import pyspark.sql.functions as F

from datalake.model.file_base import FileBase


class ETLValidation():

    class InvalidSchemaException(ValueError): pass
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
            self._to_array(kwargs.get('table')),
            self._to_array(kwargs.get('area')),
            self._to_array(kwargs.get('column'))
        )


        self._invoke_check(kwargs['check'], dfs)

    def _invoke_check(self, check, dfs):
        # call a python method by name on an instance
        # see https://stackoverflow.com/questions/3521715/call-a-python-method-by-nam
        getattr(ETLValidation(dfs[0]), check)(*dfs[1:])

    def _create_views(self, tables, areas, columns):
        return [(
            FileBase.instantiate_file(tables[i])
            .read(**self._areas_to_kwargs(areas, i))
            .select(*columns)
        ) for i in range(len(tables))]

    def _unpack_application_kwargs(self):
        # argv[1] is the script name, skip
        return json.loads(self.argv[1])

    def _to_array(self, kwarg):
        if kwarg == None:
            return []
        elif type(kwarg) == str:
            return [kwarg]
        else:
            return kwarg

    def _areas_to_kwargs(self, areas, i):
        return {'area': areas[i]} if areas else {}

if __name__ == '__main__':
    ETLValidationDispatch(sys.argv).dispatch()
