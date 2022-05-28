from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

class ETLSparkOperator(SparkSubmitOperator):
    """
    Prereqs:
    - Airflow running with following env vars same/compatible as Spark cluster:
        - JAVA_HOME (an exception thrown if not Java 8 with the 
                    pyspark version installed in Airflow's env,
                    which is 2.X, because 3.X breaks Airflow)
        - SPARK_HOME (if spark-submit and server versions 
                        different there can be non-obvious 
                        exceptions at the server)
    - Do not install the spark jobs directory inside the Airflow
        operators directory, Airflow will throw many exceptions.
    - zip datalake source files as datalake.zip, place it in 
        the spark jobs directory, and pass it to the cluster
        as a py_files argument

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
            'spark.sql.shuffle.partitions': 12,
            'spark.executor.memory': '1g'
        }

        if 'application' in  kwargs:
            application = kwargs['application']
        else:
            application = kwargs['name']
        
        # application path (the python job) is relative to AIRFLOW_HOME
        kwargs['application']=f"etl_spark_jobs/etl_{application}.py"

        kwargs['env_vars'] = self._add_dict_pair(
            kwargs.get('env_vars'),
            'DATALAKE_ROOT',
            Variable.get('datalake_root')
        )

        # upload the datalake dependencies to the spark cluster
        kwargs['py_files'] = 'etl_spark_jobs/datalake.zip'

        super().__init__(*args, **kwargs)

    @staticmethod
    def _add_dict_pair(dict, key, value):
        dict = dict if dict else {}
        dict[key] = value
        return dict