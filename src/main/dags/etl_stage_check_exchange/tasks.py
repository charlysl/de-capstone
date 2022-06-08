from airflow.models.variable import Variable

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

from dags.etl_spark.emr_tasks import EmrTasks

from datalake.utils.aws_helper import AwsHelper
from datalake.utils import spark_helper

import json


class ETLStageCheckExchangeTasks():
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

    def __init__(self):
        self.is_emr = AwsHelper.is_running_in_emr()

    def stage(self, *args, **kwargs):
        """
        Description: perform a transformation on a data set and save the
                     resulting data set in the staging area.
        """
        return self._dispatch(kwargs)

    def check(self,
                check=None,
                table=None,
                area=None,
                column=None,
                date=None,
                **kwargs):
        """
        Description: perform a validation check on a data set in the 
                     staging area.
        """

        args = {}
        args['check']=check
        args['table']=table
        args['area']=area
        args['column']=column
        args['date']=date
        kwargs['application_args']=[json.dumps(args)]

        kwargs['application']='validation'
        kwargs['py_files']="plugins/etl.py"

        return self._dispatch(kwargs)

    def exchange(self, file=None, **kwargs):
        """
        Description: load a data set from the staging area, typically either
                     into the production or into the curated area.
        """
        
        kwargs['application'] = 'load'

        # add ```file``` to application args
        kwargs['application_args'] = kwargs.pop('application_args', [])
        kwargs['application_args'].append(file)

        return self._dispatch(kwargs)


    def _dispatch(self, kwargs):
        """
        Submit a Spark job.
        
        Submit to, based on the DE_CAPSTONE_IS_EMR environment variable value:
        
        - 'false': a Spark cluster (could be local) as configured by 
                   the Airflow 'spark' connection.

                   This is very convenient for development, as one can
                   execute everything in the local machine.

        - 'true': an AWS EMR Spark cluster, as configured in ~/.aws
        
                   This is needed to run integration tests with Spark in
                   the cloud, or for production.
        """
        self._process_kwargs(kwargs)
        
        if self.is_emr:
            name = kwargs['name']
            task_id = kwargs['task_id']
            cmd = ETLStageCheckExchangeTasks._spark_submit_cmd(kwargs)
            return EmrTasks.add_emr_step(name, task_id, cmd)
        else:
            return SparkSubmitOperator(**kwargs)

    @staticmethod
    def _process_kwargs(kwargs):
        kwargs['task_id']=f"{kwargs['name']}_task"
        kwargs['conn_id']='spark'
        kwargs['verbose']=True
 
        kwargs['conf']={
            'spark.sql.shuffle.partitions': 12,
            'spark.executor.memory': '1g',
        }
        kwargs['conf'].update(spark_helper.spark_config())

        if 'application' in  kwargs:
            application = kwargs['application']
        else:
            application = kwargs['name']
        
        # application path (the python job) is relative to AIRFLOW_HOME
        kwargs['application']=f"etl_spark_jobs/etl_{application}.py"

        kwargs['env_vars'] = ETLStageCheckExchangeTasks._add_dict_pair(
            kwargs.get('env_vars'),
            'DATALAKE_ROOT',
            Variable.get('datalake_root')
        )

        kwargs['env_vars'] = ETLStageCheckExchangeTasks._add_dict_pair(
            kwargs.get('env_vars'),
            'STAGING_ROOT',
            Variable.get('staging_root')
        )

        # upload the datalake dependencies to the spark cluster
        kwargs['py_files'] = 'etl_spark_jobs/datalake.zip'

        ETLStageCheckExchangeTasks._add_application_args(kwargs)

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
    def _spark_submit_cmd(kwargs):
        """
        Description: build the spark-submit command string,
                     to configure an EMR step.

        # https://stackoverflow.com/questions/61694399/how-to-add-an-emr-spark-step
        # see: http://rmanocha.github.io/aws-emr-spark-steps.html
        # see: https://medium.com/swlh/running-pyspark-applications-on-amazon-emr-e536b7a865ca
        # see: https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921


        Example:
        spark-submit 
        --deploy-mode cluster 
        --conf spark.jars.repositories=https://repos.spark-packages.org/ 
        --conf spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0 
        --conf spark.yarn.appMasterEnv.DATALAKE_ROOT=s3a://de-capstone-2022/datalake 
        --conf spark.yarn.appMasterEnv.STAGING_ROOT=hdfs:///de-capstone-etl-staging 
        --py-files s3a://de-capstone-2022/datalake/etl_spark_jobs/datalake.zip 
        s3a://de-capstone-2022/datalake/etl_spark_jobs/etl_init_time_dim.py 
        2015-01-01
        """ 
        cmd = ['spark-submit']
        cmd.append('--deploy-mode')
        cmd.append('cluster')
        cmd.append('--conf')
        cmd.append('spark.jars.repositories=https://repos.spark-packages.org/')
        cmd.append('--conf')
        #cmd.append('spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0')
        cmd.append('spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.8.5')
        cmd.append('--conf')
        cmd.append(f"spark.yarn.appMasterEnv.DATALAKE_ROOT={Variable.get('datalake_root')}")
        cmd.append('--conf')
        cmd.append(f"spark.yarn.appMasterEnv.STAGING_ROOT={Variable.get('staging_root')}")
        cmd.append('--verbose')
        cmd.append('--py-files')
        cmd.append(f"{Variable.get('datalake_root')}/{kwargs['py_files']}")
        cmd.append(f"{Variable.get('datalake_root')}/{kwargs['application']}")
        cmd.extend(kwargs['application_args'])

        return cmd

