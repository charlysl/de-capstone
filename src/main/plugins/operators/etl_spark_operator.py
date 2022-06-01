from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

from configparser import ConfigParser
import os


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
            'spark.executor.memory': '1g',
        }
        kwargs['conf'].update(ETLSparkOperator._spark_config())

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

        kwargs['env_vars'] = self._add_dict_pair(
            kwargs.get('env_vars'),
            'STAGING_ROOT',
            Variable.get('staging_root')
        )

        # upload the datalake dependencies to the spark cluster
        kwargs['py_files'] = 'etl_spark_jobs/datalake.zip'

        ETLSparkOperator._add_application_args(kwargs)

        super().__init__(*args, **kwargs)

    @staticmethod
    def _add_dict_pair(dict, key, value):
        dict = dict if dict else {}
        dict[key] = value
        return dict

    @staticmethod
    def _add_application_args(kw_args):
        """
        From: https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/operators/spark_submit_operator/index.h
        application_args (list) - Arguments for the application being submitted (**templated**)
        """
        date = "{{ds}}"
        if 'application_args' in kw_args:
            kw_args['application_args'].append(date)
        else:
            kw_args['application_args'] = [date]

    @staticmethod
    def _spark_config():
        spark_packages = [
            'saurfang:spark-sas7bdat:3.0.0-s_2.12',
            'org.apache.hadoop:hadoop-aws:3.2.0'
        ]

        config = ConfigParser()
        aws_credentials = f'{os.environ["HOME"]}/.aws/credentials'
        config.read(aws_credentials)

        config = {
            'spark.jars.repositories': 'https://repos.spark-packages.org/',
            'spark.jars.packages': ','.join(spark_packages),
            'spark.hadoop.fs.s3a.access.key': config['default'].get('aws_access_key_id'),
            'spark.hadoop.fs.s3a.secret.key': config['default'].get('aws_secret_access_key')
        }

        return config