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
  schedule_interval='@once',
  start_date=datetime(2016,1,1),
  max_active_tasks=20
)

start_pipeline_task = DummyOperator(
  task_id='start_pipeline',
  dag=dag
)

start_spark_task = start_spark(dag)
stop_spark_task = stop_spark(dag)
preprocess_i94_data_dictionary_task = (
    create_preprocess_i94_data_dictionary_task(dag)
)
clean_states_task = create_spark_task(dag, 'clean_states')
clean_ports_task = create_spark_task(dag, 'clean_ports')
clean_country_task = create_spark_task(dag, 'clean_country')

end_pipeline_task = DummyOperator(
  task_id='end_pipeline',
  dag=dag
)

start_pipeline_task >> start_spark_task
start_spark_task >> preprocess_i94_data_dictionary_task
start_spark_task >> clean_states_task
preprocess_i94_data_dictionary_task >> clean_ports_task
preprocess_i94_data_dictionary_task >> clean_country_task
clean_ports_task >> stop_spark_task
clean_states_task >> stop_spark_task
clean_country_task >> stop_spark_task
stop_spark_task >> end_pipeline_task