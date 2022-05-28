from operators.etl_spark_operator import ETLSparkOperator

import json

class ETLCheckBaseOperator(ETLSparkOperator):
    def __init__(self,
                check=None,
                table=None,
                kind=None,
                column=None,
                date=None,
                **kwargs):

        args = {}
        args['check']=check
        args['table']=table
        args['kind']=kind
        args['column']=column
        args['date']=date
        kwargs['application_args']=[json.dumps(args)]

        kwargs['application']='validation'
        kwargs['py_files']="plugins/etl.py"
        super().__init__(**kwargs)
