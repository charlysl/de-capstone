from airflow.models import DAG

from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from etl_spark_standalone import start_spark, stop_spark


dag = DAG(
    dag_id='de-capstone-sas',
    start_date=datetime(2016, 1, 1),
    schedule_interval='@once',
    max_active_runs=1
)

split_sas_task = SparkSubmitOperator(
    task_id='split_sas',
    conn_id='spark',
    application=f"dags/spark_jobs/etl_pre_split_sas.py",
    application_args=["{{ds}}"],
    packages='saurfang:spark-sas7bdat:3.0.0-s_2.12',
    repositories='https://repos.spark-packages.org/',
    verbose=True,
    dag=dag
)

#start_spark_task = start_spark(dag)
#stop_spark_task = stop_spark(dag)

#start_spark_task >> split_sas_task
#split_sas_task >> stop_spark_task