import sys
import re

from datalake.model.file_base import FileBase


"""
Load dimension given by argv[1] from staging into production.
"""

def load_dimension():
    file = get_file(sys.argv[1])
    df = file.read(area=FileBase.staging)
    file.save(df, area=FileBase.production)

def get_file(file_class_name):
    file_module = 'datalake.datamodel.files'
    pfn = get_python_file_name(file_class_name)
    file_module_class = f'{file_module}.{pfn}.{file_class_name}'
    print('file_module_class', file_module_class)
    file = FileBase.instantiate_file(file_module_class)
    return file


def get_python_file_name(file_class):
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

load_dimension()