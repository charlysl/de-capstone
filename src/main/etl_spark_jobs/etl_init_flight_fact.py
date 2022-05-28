from pyspark.sql import DataFrame
import pyspark.sql.types as T

from datalake.datamodel.files.flight_fact_file import FlightFactFile

FlightFactFile().init()