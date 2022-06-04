from calendar import month
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

from etl_spark_standalone import start_spark, stop_spark
import stage_check_exchange_task_group as sce

from operators.etl_spark_operator import ETLSparkOperator

from datalake.datamodel.files.immigration_file import ImmigrationFile
from datalake.datamodel.files.time_dim_file import TimeDimFile
from datalake.datamodel.files.route_dim_file import RouteDimFile
from datalake.datamodel.files.visitor_dim_file import VisitorDimFile
from datalake.datamodel.files.flight_fact_file import FlightFactFile


dag = DAG(
  'de-capstone-etl-incremental',
  schedule_interval='@once',
  start_date=datetime(2016,1,1),
  end_date=datetime(2016,12,1),
  # see https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation
  max_active_runs=1,
  default_args={
      'date': '{{ds}}'
  }
)

with dag:
    start_pipeline_task = DummyOperator(task_id='start_pipeline')
    end_pipeline_task = DummyOperator(task_id='end_pipeline')
    start_spark_task = start_spark()
    stop_spark_task = stop_spark()
    clean_immigration_task = sce.create_task_group('clean_immigration', ImmigrationFile)
    with TaskGroup(
            group_id='transform_facts_and_dims',
            prefix_group_id=False
        ) as transform_facts_and_dims_group:
        dim_time_task = sce.create_task_group('dim_time', TimeDimFile)
        dim_route_task = sce.create_task_group('dim_route', RouteDimFile)
        dim_foreign_visitor_task = sce.create_task_group('dim_foreign_visitor', VisitorDimFile)
        fact_flight_task = sce.create_task_group('fact_flight', FlightFactFile)

        dim_time_task >> fact_flight_task
        dim_route_task >> fact_flight_task
        dim_foreign_visitor_task >> fact_flight_task
    

start_pipeline_task >> start_spark_task
start_spark_task >> clean_immigration_task 
clean_immigration_task >> transform_facts_and_dims_group
#fact_flight >> flight_fact_validation

transform_facts_and_dims_group >> stop_spark_task

stop_spark_task >> end_pipeline_task