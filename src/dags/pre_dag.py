from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from datetime import datetime

from etl_spark_standalone import start_spark, stop_spark
from etl_airflow_tasks import create_preprocess_i94_data_dictionary_task
from etl_airflow_tasks import create_spark_task

dag = DAG(
  'de-capstone-pre',
  schedule_interval='@once',
  start_date=datetime(2015,1,1),
  # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
  #max_active_tasks=20,
  concurrency=20
)

start_pipeline_task = DummyOperator(
  task_id='start_pipeline',
  dag=dag
)

end_preprocessing_task = DummyOperator(
  task_id='end_preprocessing',
  dag=dag
)

start_spark_task = start_spark(dag)
stop_spark_task = stop_spark(dag)

pre_states_task = create_spark_task(dag, 'pre_states')
pre_i94_data_dictionary_task = (
    create_preprocess_i94_data_dictionary_task(dag)
)

clean_airport_task = create_spark_task(dag, 'clean_airport')
clean_demographics_task = create_spark_task(dag, 'clean_demographics')
clean_temperature_task = create_spark_task(dag, 'clean_temperature')
clean_ports_task = create_spark_task(dag, 'clean_ports')
clean_country_task = create_spark_task(dag, 'clean_country')

clean_port_to_airport = create_spark_task(
    dag,
    'clean_port_to_airport',
    py_files='dags/spark_jobs/stopwords.py'
    )

dim_init_task = create_spark_task(dag, 'dim_init')
fact_init_task = create_spark_task(dag, 'fact_init')

end_pipeline_task = DummyOperator(
  task_id='end_pipeline',
  dag=dag
)

start_pipeline_task >> start_spark_task

start_spark_task >> pre_states_task
start_spark_task >> pre_i94_data_dictionary_task
start_spark_task >> dim_init_task
start_spark_task >> fact_init_task

pre_states_task >> end_preprocessing_task
pre_i94_data_dictionary_task >> end_preprocessing_task
dim_init_task >> end_preprocessing_task
fact_init_task >> end_preprocessing_task

end_preprocessing_task >> clean_airport_task
end_preprocessing_task >> clean_country_task
end_preprocessing_task >> clean_demographics_task
end_preprocessing_task >> clean_ports_task
end_preprocessing_task >> clean_temperature_task

clean_airport_task >> clean_port_to_airport
clean_ports_task >> clean_port_to_airport

clean_demographics_task >> stop_spark_task
clean_temperature_task >> stop_spark_task
clean_country_task >> stop_spark_task
clean_port_to_airport >> stop_spark_task

stop_spark_task >> end_pipeline_task