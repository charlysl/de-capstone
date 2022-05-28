from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

class ETLSparkOperator(SparkSubmitOperator):
    """
    Based on "Unit test for custom operator" in:
    https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html

    NOTES:
    - One drawback is that the context can't be accessed within the constructor.
    """
    def __init__(
        self,
        *args,
        **kwargs
    ):
        kwargs['task_id']=f"{kwargs['name']}_task"
        kwargs['conn_id']='spark'
        kwargs['verbose']=True
        kwargs['conf']={
            'spark.sql.shuffle.partitions': 5,
            'spark.executor.memory': '1g'
        }

        if 'application' in  kwargs:
            application = kwargs['application']
        else:
            application = kwargs['name']
        kwargs['application']=f"etl_spark_jobs/etl_{application}.py"

        kwargs['env_vars'] = self._add_dict_pair(
            kwargs.get('env_vars'),
            'DATALAKE_ROOT',
            Variable.get('datalake_root')
        )

        super().__init__(*args, **kwargs)

    @staticmethod
    def _add_dict_pair(dict, key, value):
        dict = dict if dict else {}
        dict[key] = value
        return dict