from calendar import month
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

from etl_spark_standalone import start_spark, stop_spark
from etl_airflow_tasks import create_preprocess_i94_data_dictionary_task

from operators.etl_spark_operator import ETLSparkOperator
from operators.etl_load_dim_operator import ETLLoadDimOperator
from operators.etl_load_fact_operator import ETLLoadFactOperator
#from operators.check_not_empty_operator import CheckNotEmptyOperator
#from operators.check_no_nulls_operator import CheckNoNullsOperator
#from operators.check_no_duplicates_operator import CheckNoDuplicatesOperator
#from operators.check_referential_integrity_operator import CheckReferentialIntegrityOperator

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
    clean_immigration_task = ETLSparkOperator(
        name='clean_immigration',
        packages='saurfang:spark-sas7bdat:3.0.0-s_2.12',
        repositories='https://repos.spark-packages.org/'
    )
    with TaskGroup(
            group_id='transform_facts_and_dims'
        ) as transform_facts_and_dims_group:
        dim_time_task = ETLSparkOperator(name='dim_time')
        dim_route_task = ETLSparkOperator(name='dim_route')
        dim_foreign_visitor_task = ETLSparkOperator(name='dim_foreign_visitor')
        fact_flight_task = ETLSparkOperator(name='fact_flight')

        dim_time_task >> fact_flight_task
        dim_route_task >> fact_flight_task
        dim_foreign_visitor_task >> fact_flight_task
    
    load_dim_time_task = ETLLoadDimOperator(
        name='load_dim_time',
        file='TimeDimFile'
    )
    load_dim_route_task = ETLLoadDimOperator(
        name='load_dim_route',
        file='RouteDimFile'
    )
    load_dim_foreign_visitor_task = ETLLoadDimOperator(
        name='load_dim_foreign_visitor',
        file='VisitorDimFile'
    )
    load_fact_flight_task = ETLLoadFactOperator(
        name='load_fact_flight',
        file='FlightFactFile'
    )

    """
    with TaskGroup('flight_fact_validation', dag=dag) as flight_fact_validation:
        check_not_empty_flight_fact = CheckNotEmptyOperator(
            name='check_not_empty_flight_fact',
            table='flight_fact',
            kind='fact',
            dag=dag
        )

        check_no_nulls_flight_fact = CheckNoNullsOperator(
            name='check_no_nulls_flight_fact',
            table='flight_fact',
            kind='fact',
            column=['time_id', 'route_id', 'visitor_id', 'num_visitors'],
            dag=dag
        )

        check_no_duplicates_flight_fact_pk = CheckNoDuplicatesOperator(
            name='check_no_duplicates_flight_fact_pk',
            table='flight_fact',
            kind='fact',
            column=['time_id', 'route_id', 'visitor_id'],
            dag=dag
        )

        check_total_participation_flight_fact_time_dim= CheckReferentialIntegrityOperator(
            name='check_total_participation_flight_fact_time_dim',
            table=['flight_fact', 'time_dim'],
            kind=['fact', 'dim'],
            column='time_id',
            dag=dag
        )

        check_total_participation_flight_fact_route_dim = CheckReferentialIntegrityOperator(
            name='check_total_participation_flight_fact_route_dim',
            table=['flight_fact', 'route_dim'],
            kind=['fact', 'dim'],
            column='route_id',
            dag=dag
        )

        check_total_participation_flight_fact_visitor_dim = CheckReferentialIntegrityOperator(
            name='check_total_participation_flight_fact_visitor_dim',
            table=['flight_fact', 'foreign_visitor_dim'],
            kind=['fact', 'dim'],
            column='visitor_id',
            dag=dag
        )
    """


start_pipeline_task >> start_spark_task
start_spark_task >> clean_immigration_task 
clean_immigration_task >> transform_facts_and_dims_group
#fact_flight >> flight_fact_validation

transform_facts_and_dims_group >> load_dim_time_task
transform_facts_and_dims_group >> load_dim_route_task
transform_facts_and_dims_group >> load_dim_foreign_visitor_task
load_dim_time_task >> load_fact_flight_task
load_dim_route_task >> load_fact_flight_task
load_dim_foreign_visitor_task  >> load_fact_flight_task

load_fact_flight_task >> stop_spark_task

stop_spark_task >> end_pipeline_task