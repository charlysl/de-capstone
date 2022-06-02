import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('state_id', T.StringType(), True),
    T.StructField('name', T.StringType(), True),
    T.StructField('type_id', T.IntegerType(), True),
    T.StructField('type', T.StringType(), True),
])

class StatesFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "states",
            schema,
            self.curated,
            writable=True
        )

    def get_checks(self):
        return [{
            'check': 'check_not_empty',
            'table': 'datalake.datamodel.files.states_file.StatesFile',
            'area': 'staging',
        }]
