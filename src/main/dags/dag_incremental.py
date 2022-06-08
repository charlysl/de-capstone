from calendar import month
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

from dags.etl_spark import spark_tasks

from etl_stage_check_exchange.pattern import ETLStageCheckExchange

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.time_dim_file import TimeDimFile
from datalake.datamodel.files.route_dim_file import RouteDimFile
from datalake.datamodel.files.visitor_dim_file import VisitorDimFile
from datalake.datamodel.files.flight_fact_file import FlightFactFile


sce = ETLStageCheckExchange()

dag = DAG(
  'de-capstone-etl-incremental',
  schedule_interval='@once',
  start_date=datetime(2016,1,1),
  end_date=datetime(2016,12,1),
  # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
  max_active_runs=1,
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

    clean_immigration_task = sce.create('clean_immigration', ImmigrationFile)
    with TaskGroup(
            group_id='transform_facts_and_dims',
            prefix_group_id=False
        ) as transform_facts_and_dims_group:
        dim_time_task = sce.create('dim_time', TimeDimFile)
        dim_route_task = sce.create('dim_route', RouteDimFile)
        dim_foreign_visitor_task = sce.create('dim_foreign_visitor', VisitorDimFile)
        fact_flight_task = sce.create('fact_flight', FlightFactFile)

        dim_time_task >> fact_flight_task
        dim_route_task >> fact_flight_task
        dim_foreign_visitor_task >> fact_flight_task
    

start_pipeline_task >> start_spark_task
start_spark_task >> wait_for_spark_ready

wait_for_spark_ready >> clean_immigration_task
clean_immigration_task >> transform_facts_and_dims_group
transform_facts_and_dims_group >> stop_spark_task

stop_spark_task >> end_pipeline_task