import sys
import re

from datalake.model.file_base import FileBase


"""
Load dataset given by argv[1] from staging.
"""

def load():
    file_class = FileBase.get_class_from_class_name(sys.argv[1])
    file = file_class()
    df = file.read(area=FileBase.staging)
    file.save(df)

load()