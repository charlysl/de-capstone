import datalake.utils.pipe

class DatalakeFile():

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
                datalake,
                area,
                format='parquet',
                writable=False,
                mode='append',
                options=None
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
        self.datalake = datalake
        self.area = area
        self.writable = writable
        self.mode = mode
        self.format = format
        self.options = options
    
    def read(self):
        return (
            self.datalake.get_spark().read
            .pipe(self._set_options, self.options)
            .format(self.format)
            .schema(self.schema)
            .load(self._path())
        )

    def save(self, df, partitions=None):
        """
        - mode: default is APPEND, the safest option
        Partitioning by year and month would allow other processes
        to write other partitions in parallel.
        """
        if not self.writable:
            raise self.FileNotWritableException(self)

        (
            df.write
            .pipe(self._set_partitioning, partitions)
            .format(self.format)
            .mode(self.mode)
            .save(self._path())
        )

    def init(self, df, filename, **kwargs):
        """
        Mode is IGNORE, because it should be Idempotent, but should
        never clobber existing production tables.
        """
        super().save(df, filename, mode='IGNORE', **kwargs)

    def _path(self):
        return f"{self.datalake.root}/{self.area}/{self.name}"

    def _set_partitioning(self, df_writer, partitions):
        return df_writer.partitionBy(partitions) if partitions else df_writer

    def _set_options(self, df_io, options):
        return df_io.options(**options) if options else df_io