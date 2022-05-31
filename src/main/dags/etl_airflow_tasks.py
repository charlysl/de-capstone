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

