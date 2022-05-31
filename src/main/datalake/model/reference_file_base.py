from datalake.model.file_base import FileBase

class ReferenceFileBase(FileBase):
    def __init__(self, *args, **kwargs):
        kwargs['writable'] = True
        kwargs['mode'] = 'overwrite'
        super().__init__(*args, **kwargs)