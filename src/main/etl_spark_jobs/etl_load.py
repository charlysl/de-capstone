import sys

from datalake.model.file_base import FileBase

from datalake.utils import spark_helper


def load(file_class_name):
    """
    Description: load a file

    Parameters:
    - file_class_name: file class name

    Effects:
    - Copy file from staging area to file.area
    - If file.area is staging, do nothing
    """
    file_class = FileBase.get_class_from_class_name(file_class_name)
    file = file_class()

    if file.area == FileBase.staging:
        # Loading is always from staging, so can skip it if 
        # destination area is staging
        
        # Before returning, initialize Spark context, otherwise, in EMR:
        #```
        #java.lang.IllegalStateException:User did not initialize spark context!
        #```
        spark_helper.get_spark()
        
        return

    df = file.read(area=FileBase.staging)
    file.save(df)

if __name__ == '__main__':
    load(sys.argv[1])