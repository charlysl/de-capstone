from operators.etl_spark_operator import ETLSparkOperator

class ETLLoadOperator(ETLSparkOperator):
    def __init__(self, file=None, **kwargs):
        kwargs['application'] = 'load'

        # add ```file``` to application args
        kwargs['application_args'] = kwargs.pop('application_args', [])
        kwargs['application_args'].append(file)

        super().__init__(**kwargs)