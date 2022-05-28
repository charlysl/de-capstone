from operators.etl_check_base_operator import ETLCheckBaseOperator

import json

class CheckNoDuplicatesOperator(ETLCheckBaseOperator):
    def __init__(self, **kwargs):
        kwargs['check']='check_no_duplicates'
        super().__init__(**kwargs)
