import datalake.utils.pipe
from datalake.utils import spark_helper

import os
import importlib


class FileBase():

    @staticmethod
    def get_datalake_root_key():
        return 'DATALAKE_ROOT'

    @staticmethod
    def get_staging_root_key():
        return 'STAGING_ROOT'

    @staticmethod
    def get_datalake_root():
        return os.environ[FileBase.get_datalake_root_key()]

    @staticmethod
    def get_staging_root():
        return os.environ[FileBase.get_staging_root_key()]

    class FileNotWritableException(Exception):
        def __init__(self, file):
            super().__init__(f"File {file._path()} is not writable")

    eda = 'eda'
    raw = 'raw'
    curated = 'curated'
    staging = 'staging'
    production = 'production'

    def __init__(
                self,
                name,
                schema,
                area,
                datalake_root=None,
                staging_root=None,
                format='parquet',
                writable=False,
                mode='append',
                coalesce=None,
                options=None,
                partitions=None
                ):
        """
        Parameters:
        - schema:   - If pyspark.sql.StructType: schema-on-read, the file's 
                      Spark DataFrame schema.
                    - If None: schema-on-write, and it is a prereq that the 
                      format is parquet.
        """
        self.name = name
        self.schema = schema
        self.datalake_root = (
            datalake_root if datalake_root else FileBase.get_datalake_root()
        )
        self.staging_root = (
            staging_root if staging_root else FileBase.get_staging_root()
        )
        self.area = area
        self.writable = writable
        self.mode = mode
        self.format = format
        self.coalesce = coalesce
        self.options = options
        self.partitions = partitions
    
    def read(self, area=None):
        return (
            spark_helper.get_spark().read
            .pipe(self._set_options, self.options)
            .format(self.format)
            .schema(self.schema)
            .load(self._path(area))
        )

    def save(self, df, area=None, mode=None, force=False):
        """
        - mode: default is APPEND, the safest option
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        - force: ignore self.writable; intended for testing
        """
        if not force and not self.writable:
            raise self.FileNotWritableException(self)

        (
            df
            .pipe(self._coalesce)
            .write
            .pipe(self._set_partitioning)
            .format(self.format)
            .mode(mode if mode else self.mode)
            .save(self._path(area))
        )

    def init(self):
        """
        Mode is IGNORE, because it should be Idempotent, but should
        never clobber existing production tables,
        like CREATE IF NOT EXISTS
        """
        self.save(self._create_empty_dataframe(), mode='ignore')

    def _path(self, area=None):
        area = area if area else self.area

        if area == 'staging':
            return self._staging_path()
        else:
            return f"{self.datalake_root}/{area}/{self.name}"

    def _staging_path(self):
        return f"{self.staging_root}/{self.name}"

    def _set_partitioning(self, df_writer):
        partitions = self.partitions
        return df_writer.partitionBy(partitions) if partitions else df_writer

    def _coalesce(self, df):
        return df.coalesce(self.coalesce) if self.coalesce else df

    def _set_options(self, df_io, options):
        return df_io.options(**options) if options else df_io

    def _create_empty_dataframe(self):
        return spark_helper.get_spark().createDataFrame([], self.schema)

    @staticmethod
    def instantiate_file(file_module_class):
        """
        Description: instantiate given file class

        Parameters: a string of file's module and class anem

        Returns: an instance of the file class

        Example:
        file_module_class = 'datalake.datamodel.files.states_file.StatesFile'
        module_name: 'datalake.datamodel.files.states_file'
        class_name: 'StatesFile'
        """
        module_name = '.'.join(file_module_class.split('.')[:-1])
        class_name = file_module_class.split('.')[-1]
        module = importlib.import_module(module_name)
        file_class = getattr(module, class_name)
        file = file_class()
        return file

