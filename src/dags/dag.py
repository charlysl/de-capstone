from calendar import month
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

from etl_spark_standalone import start_spark, stop_spark
from etl_airflow_tasks import create_preprocess_i94_data_dictionary_task
from etl_airflow_tasks import create_spark_task

dag = DAG(
  'de-capstone',
  schedule_interval='@monthly',
  start_date=datetime(2016,1,1),
  end_date=datetime(2016,12,1),
  # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
  #max_active_tasks=20,
  max_active_runs=1,
  concurrency=20
)

start_pipeline_task = DummyOperator(
  task_id='start_pipeline',
  dag=dag
)

start_spark_task = start_spark(dag)
stop_spark_task = stop_spark(dag)

clean_immigration_task = create_spark_task(
    dag,
    'clean_immigration',
    #packages='saurfang:spark-sas7bdat:3.0.0-s_2.12',
    packages='saurfang:spark-sas7bdat:3.0.0-s_2.11',
    repositories='https://repos.spark-packages.org/',
    py_files='dags/spark_jobs/age.py,dags/spark_jobs/stay.py'
)

dim_time_task = create_spark_task(dag, 'dim_time')
dim_route_task = create_spark_task(dag, 'dim_route')
dim_foreign_visitor_task = create_spark_task(
    dag,
    'dim_foreign_visitor',
    py_files='dags/spark_jobs/age.py,dags/spark_jobs/stay.py'
    )

fact_flight = create_spark_task(dag, 'fact_flight')

end_pipeline_task = DummyOperator(
  task_id='end_pipeline',
  dag=dag
)

start_pipeline_task >> start_spark_task

start_spark_task >> clean_immigration_task 

clean_immigration_task >> dim_time_task
clean_immigration_task >> dim_route_task
clean_immigration_task >> dim_foreign_visitor_task

dim_time_task >> fact_flight
dim_route_task >> fact_flight
dim_foreign_visitor_task >> fact_flight

fact_flight >> stop_spark_task

stop_spark_task >> end_pipeline_task
