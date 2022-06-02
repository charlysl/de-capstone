import pyspark.sql.types as T

from datalake.model.reference_file_base import ReferenceFileBase

schema = T.StructType([
    T.StructField('country_id', T.IntegerType(), True),
    T.StructField('country', T.StringType(), True),
])

class CountryFile(ReferenceFileBase):
    def __init__(self):
        super().__init__(
            "country",
            schema,
            self.curated
        )

        self.add_check(self.Check.not_empty)
        self.add_check(self.Check.no_nulls, column='country_id')
        self.add_check(self.Check.no_nulls, column='country')
        self.add_check(self.Check.no_duplicates, column='country_id')
        #self.add_check(self.Check.no_duplicates, column='country')
        self.add_check(self.Check.no_duplicates, column=['country_id', 'country'])
        
