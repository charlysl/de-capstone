from operators.etl_check_base_operator import ETLCheckBaseOperator

import json

class CheckNoNullsOperator(ETLCheckBaseOperator):
    def __init__(self, **kwargs):
        kwargs['check']='check_no_nulls'
        super().__init__(**kwargs)
