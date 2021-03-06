from datalake.model.file_base import FileBase
from datalake.utils import test_utils

class TestStagingFile(FileBase):
    """
    A dummy file for testing.
    """
    def __init__(self, **kwargs):
        super().__init__(
            'test',
            test_utils.create_schema(1),
            self.staging,
            writable=True,
            **kwargs
        )