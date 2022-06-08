import datalake.utils.pipe
from datalake.utils import spark_helper

import os
import re
import copy
import importlib
import logging

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

    class Check:
        not_empty = 'check_not_empty'
        no_nulls = 'check_no_nulls'
        no_duplicates = 'check_no_duplicates'
        referential_integrity = 'check_referential_integrity'

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
        self.datalake_root = datalake_root
        self.staging_root = staging_root
        self.area = area
        self.writable = writable
        self.mode = mode
        self.format = format
        self.coalesce = coalesce
        self.options = options
        self.partitions = partitions

        # init checks
        self.checks = []
        self.base_check = {
            'table': [self.__class__.__name__],
        }

    
    def read(self, area=None):

        logging.info(f'READING {self._path(area)}')

        return (
            spark_helper.get_spark().read
            .pipe(self._set_options, self.options)
            .format(self.format)
            .pipe(lambda reader: (
                reader.schema(self.schema) if self.schema else reader)
            )
            .load(self._path(area))
        )

    def save(self, df, area=None, mode=None, force=False):
        """
        - mode: default is APPEND, the safest option
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        - force: ignore self.writable; intended for testing
        """
        mode = mode if mode else self.mode

        logging.info(f'SAVING(mode={mode}) {self._path(area)}')

        if not force and not self.writable:
            raise self.FileNotWritableException(self)

        (
            df
            .pipe(self._coalesce)
            .write
            .pipe(self._set_partitioning)
            .format(self.format)
            .mode(mode)
            .save(self._path(area))
        )

    def stage(self, df):
        """
        Description: save in staging area.

        Parameters:
        - df: DataFrame - the data frame to be saved.

        Effects: overwrite the dataset in the staging area.
        """
        self.save(df, area=FileBase.staging, mode='overwrite')

    def init(self):
        """
        Mode is IGNORE, because it should be Idempotent, but should
        never clobber existing production tables,
        like CREATE IF NOT EXISTS
        """
        self.save(self._create_empty_dataframe(), mode='ignore')

    def get_checks(self):
        """
        Description: get validation checks to be performed on this dataset.

        Returns: a list of dictionaries that describes one check.
        ```
            list({
                    'check': str,
                    'dataset': str | list [str],
                    'column': str | list[str],
                    'area': str
            })
        ```
        """
        return self.checks

    def add_check(self, check, column=None, table=None):
        new_check = copy.deepcopy(self.base_check)
        new_check['check'] = check
        if column:
            new_check['column'] = column
        if table:
            tables = [table] if type(table) == str else table
            # table routeDim would become RouteDimFile:
            new_check['table'].extend([f'{t[0].upper()}{t[1:]}File' for t in tables])
        self.get_checks().append(new_check)


    def try_to_read_from_staging(self):
        """
        Description: try to read the file from the staging area

        Returns: DataFrame of file

        Effects:
        - if the file is already in staging: read it
        - otherwise: read it from its default area=
        """

        """
        This behaviour should be efficient enough:
        - reference tables are typically small, so it shouldn't
        be too inneficient to repeatedly read them from curated.
        - dimension copies and partial fact tables should be in staging
        anyway. It is because of these that it is important to try
        to read the file from staging first, because otherwise we
        would be reading the old dimensions and the whole facts, wrongly.
        - an alternative design would have been to configure in the
        checks from which are to read each file from.
        """
        try:
            df = self.read(area=FileBase.staging)
        except Exception:
            # most likely the file was not in staging
            df = self.read()
        return df

    def _path(self, area=None):
        area = area if area else self.area

        if area == 'staging':
            return self._staging_path()
        else:
            return f"{self._get_datalake_root()}/{area}/{self.name}"

    def _staging_path(self):
        return f"{self._get_staging_root()}/{self.name}"

    def _set_partitioning(self, df_writer):
        partitions = self.partitions
        return df_writer.partitionBy(partitions) if partitions else df_writer

    def _coalesce(self, df):
        return df.coalesce(self.coalesce) if self.coalesce else df

    def _set_options(self, df_io, options):
        return df_io.options(**options) if options else df_io

    def _create_empty_dataframe(self):
        return spark_helper.get_spark().createDataFrame([], self.schema)

    def _get_datalake_root(self):
        return self.datalake_root if self.datalake_root else FileBase.get_datalake_root()

    def _get_staging_root(self):
        return self.staging_root if self.staging_root else FileBase.get_staging_root()


    @staticmethod
    def instantiate_file(file_module_class):
        """
        Description: get data set class that corresponds to given name

        Parameters: a string of file's module and class anem

        Returns: the file's class

        Example:
        file_module_class = 'datalake.datamodel.files.states_file.StatesFile'
        module_name: 'datalake.datamodel.files.states_file'
        class_name: 'StatesFile'

        TODO: rename to ```get_class```
        """
        file_class = FileBase.get_class_from_full_name(file_module_class)
        return file_class()

    @staticmethod
    def get_class_from_full_name(file_module_class):
        module_name = '.'.join(file_module_class.split('.')[:-1])
        class_name = file_module_class.split('.')[-1]
        module = importlib.import_module(module_name)
        file_class = getattr(module, class_name)
        return file_class

    @staticmethod
    def get_class_from_class_name(file_class_name):
        file_module = 'datalake.datamodel.files'
        pfn = FileBase._get_python_file_name(file_class_name)
        file_module_class = f'{file_module}.{pfn}.{file_class_name}'
        file_class = FileBase.get_class_from_full_name(file_module_class)
        return file_class

    @staticmethod
    def _get_python_file_name(file_class):
        """
        Produce the name of the python file that contains given file class

        Example:
        Given 'TimeDimFile', produce 'time_dim_file'
        """
        # assume file_class in camel case
        # i.e. TimeDimFile
        tokens = re.split('([A-Z][^A-Z]+)', file_class)

        # remove empty tokens
        words = filter(lambda s: len(s) > 0, tokens)

        # join words
        # i.e. time_dim_file
        return '_'.join(words).lower()