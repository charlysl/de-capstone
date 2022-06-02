from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from datetime import datetime

from etl_spark_standalone import start_spark, stop_spark
from etl_airflow_tasks import create_preprocess_i94_data_dictionary_task
from operators.etl_spark_operator import ETLSparkOperator
import stage_check_exchange_task_group as sce

from datalake.datamodel.files.states_file import StatesFile


dag = DAG(
  'de-capstone-etl-initial',
  schedule_interval='@once',
  start_date=datetime(2015,1,1), # ensure starts before incremental dag
  # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
  max_active_runs=1
)


with dag:
    start_pipeline_task = DummyOperator(task_id='start_pipeline')
    end_pipeline_task = DummyOperator(task_id='end_pipeline')
    stop_spark_task = stop_spark()
    start_spark_task = start_spark()

    with TaskGroup(group_id='init_dims_and_facts', prefix_group_id=False) as init_dims_and_facts_group:
        init_time_dim_task = ETLSparkOperator(name='init_time_dim')
        init_route_dim_task = ETLSparkOperator(name='init_route_dim')
        init_foreign_visitor_dim_task = ETLSparkOperator(name='init_foreign_visitor_dim')
        init_flight_fact_task = ETLSparkOperator(name='init_flight_fact')

    with TaskGroup(group_id='reference_tables_preprocessing', prefix_group_id=False) as reference_tables_preprocessing_group:
        #clean_states_task = ETLSparkOperator(dag=dag, name='clean_states')
        clean_states_task = sce.create_task_group('clean_states', StatesFile)

        process_i94_data_dictionary_task = (
            create_preprocess_i94_data_dictionary_task(dag)
        )

    with TaskGroup(group_id='create_reference_tables', prefix_group_id=False) as create_reference_tables_group:
        clean_airport_task = ETLSparkOperator(name='clean_airports')
        clean_demographics_task = ETLSparkOperator(name='clean_demographics')
        clean_temperature_task = ETLSparkOperator(name='clean_temperatures')
        clean_ports_task = ETLSparkOperator(name='clean_ports')
        clean_country_task = ETLSparkOperator(name='clean_country')
        clean_port_to_airport = ETLSparkOperator(name='clean_ports_to_airports')

        clean_airport_task >> clean_port_to_airport
        clean_ports_task >> clean_port_to_airport


start_pipeline_task >> start_spark_task
start_spark_task >> init_dims_and_facts_group
start_spark_task >> reference_tables_preprocessing_group
reference_tables_preprocessing_group >> create_reference_tables_group
init_dims_and_facts_group >> stop_spark_task
create_reference_tables_group >> stop_spark_task
stop_spark_task >> end_pipeline_task
