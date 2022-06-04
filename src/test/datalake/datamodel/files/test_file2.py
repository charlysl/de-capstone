from datalake.model.file_base import FileBase
from datalake.utils import test_utils

class TestFile2(FileBase):
    """
    A dummy file for testing.
    """
    def __init__(self, **kwargs):
        super().__init__(
            'test2',
            test_utils.create_schema(2),
            self.curated,
            writable=True,
            **kwargs
        )