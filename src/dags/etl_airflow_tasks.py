from email.mime import application
from re import sub
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from etl_preprocess_i94_data_dictionary import preprocess_i94_data_dictionary

def create_preprocess_i94_data_dictionary_task(dag):
    return PythonOperator(
        task_id='i94_data_dictionary_task',
        python_callable=preprocess_i94_data_dictionary,
        dag=dag
    )

def create_spark_task(dag, name, py_files=None):
    return SparkSubmitOperator(
        task_id=f"{name}_task",
        conn_id='spark',
        application=f"dags/spark_jobs/etl_{name}.py",
        verbose=True,
        py_files=py_files,
        dag=dag
    )

