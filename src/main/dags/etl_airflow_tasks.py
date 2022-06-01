import os
from re import sub
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator

from airflow.models.variable import Variable

def dummy_script(*args, **kwargs):
    print('dummy_script_args', args)
    print('dummy_script_kwargs', kwargs)
    return '{"a": 1}'

def create_preprocess_i94_data_dictionary_task(dag):
    """
    Description: preprocess the i94 data dictionary.

    Returns: the effect is the same in both cases
    - If the datalake is in S3: a S3FileTransformOperator
    - If the datalke is in the local file system: a python operator
    """

    task_id = 'i94_data_dictionary_task'

    if _is_datalake_in_S3():
        
        return S3FileTransformOperator(
            task_id=task_id,
            source_s3_key='s3://de-capstone-2022/raw/I94_SAS_Labels_Descriptions.SAS',
            dest_s3_key='s3://de-capstone-2022/curated/i94_data_dictionary.json',
            transform_script=_get_script(),
            replace=True,
            dag=dag
        )
    else:
        from etl_preprocess_i94_data_dictionary import preprocess_i94_data_dictionary
        return PythonOperator(
            task_id=task_id,
            python_callable=preprocess_i94_data_dictionary,
            dag=dag
        )

def _is_datalake_in_S3():
    datalake_root = Variable.get('datalake_root')
    return len(datalake_root) > 1 and datalake_root[:2] == 's3'

def _get_script():
    script = 'dags/etl_preprocess_i94_data_dictionary.py'
    return  f'{os.environ["AIRFLOW_HOME"]}/{script}'