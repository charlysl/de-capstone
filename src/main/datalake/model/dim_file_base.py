import pyspark.sql.functions as F

from datalake.model.file_base import FileBase


class DimFileBase(FileBase):
    def __init__(self, name, schema):
        super().__init__(
            name,
            schema,
            self.production,
            writable=True,
            mode='overwrite'
        )

    # static methods
    def on(df1, df2, cols):
        cond = F.expr('true')
        for col in cols:
             cond = cond & (df1[col] == df2[col])
        return cond

    # instance methods
    def get_nk(self):
        return self.nk
    
    def keys_to_str(self, keys):
        return ', '.join([f"cast({key} as string)" for key in keys])
    
    def gen_sk_expr(self, df_keys=None):
        keys = self.get_nk() if not df_keys else df_keys
        return f"md5(concat({self.keys_to_str(keys)}))"
    
    def on_nk (self, df, dim, df_keys=None):
        """
        Assume: unique role for all keys if df_keys is None.
        Prereq: len(nk) >= len(df_keys)
        Example: df is immigration
        TODO: unit test
        """
        nk = self.get_nk()
        keys = df_keys if df_keys else nk

        cond = F.expr('true')
        for i in range(len(keys)):
             cond = cond & (df[keys[i]] == dim[nk[i]])
        return cond

    