import pyspark.sql.types as T

from datalake.model.file_base import FileBase


table_schema = T.ArrayType(T.ArrayType(T.StringType(),True),True)

schema = T.StructType([
    T.StructField('I94ADDR', table_schema, True),
    T.StructField('I94CIT', table_schema, True),
    T.StructField('I94MODE', table_schema, True),
    T.StructField('I94PORT', table_schema, True),
    T.StructField('VISATYPE', table_schema, True)
])


class I94DataDictionaryFile(FileBase):
    def __init__(self):
        super().__init__(
            "i94_data_dictionary",
            schema,
            self.curated,
            format='json',
            options={'multiline': 'true'}
        )