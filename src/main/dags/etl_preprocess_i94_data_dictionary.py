#!/usr/bin/env python3

from airflow.models.variable import Variable

import json
from pathlib import Path

from i94_data_dictionary import DataDictionaryParser, ListData


#TODO improve this a little bit, too casual

def preprocess_i94_data_dictionary(src=None, dst=None):
    if not src:
        src = _get_path('I94_SAS_Labels_Descriptions.SAS', 'raw')
    if not dst:
        dst = _get_path('i94_data_dictionary.json', 'curated')

    _preprocess_i94_data_dictionary(src, dst)

def _preprocess_i94_data_dictionary(src, dst):
    json_dict = _extract_info_from_dictionary(src)
    _create_curated_area_if_not_exists()
    with open(dst, 'w') as f:
        f.write(json_dict)

def _get_datalake_root():
    return Variable.get('datalake_root')

def _get_path(filename, area):
    datalake_root = _get_datalake_root()
    path = f'{datalake_root}/{area}/{filename}'
    return path

def _extract_info_from_dictionary(file):
    data_dict = DataDictionaryParser(file, output=ListData()).parse()
    return json.dumps(data_dict, indent=4)

def _create_curated_area_if_not_exists():
    datalake_root = _get_datalake_root()
    path = f'{datalake_root}/curated'
    Path(path).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    import sys
    preprocess_i94_data_dictionary(src=sys.argv[1], dst=sys.argv[2])