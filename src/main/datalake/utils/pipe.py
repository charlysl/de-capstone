from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter

def pipe(self, *args):
    """
    Description: a pandas-like pipe function

    Input: 
    - self: an object
    - fn: a function with signature   type(self): type(self)

    Output: an object of type(self)

    Usage:
    To make some Spark methods pipeable out of the box (see below):
    ```
    import datalake.utils.pipe
    ```
    """
    return args[0](self, *args[1:])

# TODO assert DataFrame does not have pipe method, API might add it
DataFrame.pipe = pipe
DataFrameReader.pipe = pipe
DataFrameWriter.pipe = pipe