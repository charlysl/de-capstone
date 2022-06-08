from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

from etl_airflow_tasks import create_preprocess_i94_data_dictionary_task

from etl_stage_check_exchange.pattern import ETLStageCheckExchange
from etl_stage_check_exchange.tasks import ETLStageCheckExchangeTasks

from dags.etl_spark import spark_tasks

from datalake.datamodel.files.airports_file import AirportsFile
from datalake.datamodel.files.country_file import CountryFile
from datalake.datamodel.files.demographics_file import DemographicsFile
from datalake.datamodel.files.ports_file import PortsFile
from datalake.datamodel.files.ports_to_airports_file import PortsToAirportsFile
from datalake.datamodel.files.states_file import StatesFile
from datalake.datamodel.files.temperatures_file import TemperaturesFile


sce = ETLStageCheckExchange()
sce_tasks = ETLStageCheckExchangeTasks()

dag = DAG(
    'de-capstone-etl-initial',
    schedule_interval='@once',
    start_date=datetime(2015,1,1), # ensure starts before incremental dag
    # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
    max_active_runs=1,
    concurrency=12, # should prevent timeouts
    default_args={
        'date': '{{ds}}',
        'retries': 3,
        'retry_delay': timedelta(seconds=20),
    }
)


with dag:
    start_pipeline_task = DummyOperator(task_id='start_pipeline')
    end_pipeline_task = DummyOperator(task_id='end_pipeline')

    start_spark_task = spark_tasks.start_spark()
    wait_for_spark_ready = spark_tasks.wait_for_spark_ready()
    stop_spark_task = spark_tasks.stop_spark()

    with TaskGroup(group_id='init_dims_and_facts', prefix_group_id=False) as init_dims_and_facts_group:
        init_time_dim_task = sce_tasks.stage(name='init_time_dim')
        init_route_dim_task = sce_tasks.stage(name='init_route_dim')
        init_foreign_visitor_dim_task = sce_tasks.stage(name='init_foreign_visitor_dim')
        init_flight_fact_task = sce_tasks.stage(name='init_flight_fact')

    with TaskGroup(group_id='reference_tables_preprocessing', prefix_group_id=False) as reference_tables_preprocessing_group:
        clean_states_task = sce.create('clean_states', StatesFile)

        process_i94_data_dictionary_task = (
            create_preprocess_i94_data_dictionary_task(dag)
        )

    with TaskGroup(group_id='create_reference_tables', prefix_group_id=False) as create_reference_tables_group:
        clean_airport_task = sce.create('clean_airports', AirportsFile)
        clean_demographics_task = sce.create('clean_demographics', DemographicsFile)
        clean_temperature_task = sce.create('clean_temperatures', TemperaturesFile)
        clean_ports_task = sce.create('clean_ports', PortsFile)
        clean_country_task = sce.create('clean_country', CountryFile)
        clean_port_to_airport = sce.create('clean_ports_to_airports', PortsToAirportsFile)

        clean_airport_task >> clean_port_to_airport
        clean_ports_task >> clean_port_to_airport


start_pipeline_task >> start_spark_task
start_spark_task >> wait_for_spark_ready
wait_for_spark_ready >> init_dims_and_facts_group
wait_for_spark_ready >> reference_tables_preprocessing_group
reference_tables_preprocessing_group >> create_reference_tables_group
init_dims_and_facts_group >> stop_spark_task
create_reference_tables_group >> stop_spark_task
stop_spark_task >> end_pipeline_task
