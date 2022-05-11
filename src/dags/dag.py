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

clean_states_task = create_spark_task(dag, 'clean_states')
clean_airport_task = create_spark_task(dag, 'clean_airport')
clean_demographics_task = create_spark_task(dag, 'clean_demographics')
clean_temperature_task = create_spark_task(dag, 'clean_temperature')
clean_immigration_task = create_spark_task(
    dag,
    'clean_immigration',
    py_files='dags/spark_jobs/age.py,dags/spark_jobs/stay.py'
    )

preprocess_i94_data_dictionary_task = (
    create_preprocess_i94_data_dictionary_task(dag)
)

clean_ports_task = create_spark_task(dag, 'clean_ports')
clean_country_task = create_spark_task(dag, 'clean_country')

clean_port_to_airport = create_spark_task(
    dag,
    'clean_port_to_airport',
    py_files='dags/spark_jobs/stopwords.py'
    )

dim_init_task = create_spark_task(dag, 'dim_init')
dim_time_task = create_spark_task(dag, 'dim_time')
dim_route_task = create_spark_task(dag, 'dim_route')
dim_foreign_visitor_task = create_spark_task(
    dag,
    'dim_foreign_visitor',
    py_files='dags/spark_jobs/age.py,dags/spark_jobs/stay.py'
    )

fact_init = create_spark_task(dag, 'fact_init')
fact_flight = create_spark_task(dag, 'fact_flight')

end_pipeline_task = DummyOperator(
  task_id='end_pipeline',
  dag=dag
)



start_pipeline_task >> start_spark_task

start_spark_task >> clean_states_task
start_spark_task >> clean_airport_task
start_spark_task >> clean_demographics_task
start_spark_task >> clean_temperature_task
start_spark_task >> clean_immigration_task 
start_spark_task >> preprocess_i94_data_dictionary_task

preprocess_i94_data_dictionary_task >> clean_ports_task
preprocess_i94_data_dictionary_task >> clean_country_task

clean_airport_task >> clean_port_to_airport
clean_ports_task >> clean_port_to_airport

clean_states_task >> dim_init_task
clean_demographics_task >> dim_init_task
clean_temperature_task >> dim_init_task
clean_immigration_task >> dim_init_task
clean_country_task >> dim_init_task
clean_immigration_task >> dim_init_task
clean_port_to_airport >> dim_init_task

dim_init_task >> dim_time_task
dim_init_task >> dim_route_task
dim_init_task >> dim_foreign_visitor_task

dim_time_task >> fact_init
dim_route_task >> fact_init
dim_foreign_visitor_task >> fact_init

fact_init >> fact_flight

fact_flight >> stop_spark_task

stop_spark_task >> end_pipeline_task