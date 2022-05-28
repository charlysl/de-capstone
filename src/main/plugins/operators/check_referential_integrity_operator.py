from operators.etl_check_base_operator import ETLCheckBaseOperator

import json

class CheckReferentialIntegrityOperator(ETLCheckBaseOperator):
    def __init__(self, **kwargs):
        kwargs['check']='check_referential_integrity'
        super().__init__(**kwargs)
