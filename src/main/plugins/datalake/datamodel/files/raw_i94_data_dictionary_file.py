import pyspark.sql.types as T

from datalake.model.file_base import FileBase


class RawI94DataDictionaryFile(FileBase):
    def __init__(self):
        super().__init__(
            "I94_SAS_Labels_Descriptions.SAS",
            None,
            self.raw,
            format='text',
        )