import json

from etl import SparkETL
from i94_data_dictionary import DataDictionaryParser, ListData


def preprocess_i94_data_dictionary():
    etl = SparkETL('etl-preprocess-i94-data-dictionary')

    file = etl.data_sources['i94_data_dictionary']
    data_dict = DataDictionaryParser(file, output=ListData()).parse()
    json_dict =  json.dumps(data_dict, indent=4)

    with open(etl.i94_data_dictionary, 'w') as f:
        f.write(json_dict)

