from operators.etl_check_base_operator import ETLCheckBaseOperator

class CheckNotEmptyOperator(ETLCheckBaseOperator):
    def __init__(self, check=None, **kwargs):
        kwargs['check']='check_not_empty'
        super().__init__(**kwargs)
