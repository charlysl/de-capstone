from operators.etl_spark_operator import ETLSparkOperator

class ETLLoadDimOperator(ETLSparkOperator):
    def __init__(self, file=None, **kwargs):
        kwargs['application'] = 'load_dim'

        kwargs['application_args'] = kwargs.pop('application_args', [])
        kwargs['application_args'].append(file)

        super().__init__(**kwargs)