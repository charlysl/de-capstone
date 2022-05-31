from operators.etl_load_dim_operator import ETLLoadDimOperator

class ETLLoadFactOperator(ETLLoadDimOperator):
    def __init__(self, file=None, **kwargs):
        super().__init__(file=file, **kwargs)