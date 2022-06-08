import findspark
findspark.init()
from datalake.utils import spark_helper
spark_helper.get_spark()

import os

from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

import datetime
import pendulum

from datalake.model.file_base import FileBase
from datalake.utils import test_utils

from dags.etl_stage_check_exchange.tasks import ETLStageCheckExchangeTasks

import unittest


class ETLSceTestBase(unittest.TestCase):
    """
    Do not subclass ETLTestBase, because we do not
    want the DATALAKE_ROOT env var to be directly set by the test,
    but, rather, using an Airflow variable, and reading this 
    variable via the Airflow API is the operator's responsability,
    not the spark job's.

    Application path starts at dir were you execute test;
    this makes it easy to provide a dummy job that alway succeeds,
    like etl_spark_jobs/etl_test.py
    """

    def setUp(self):
        super().setUp()
        self._setup_test_dag()
        self._setup_airflow_spark_connection()
        self._mock_airflow_variables()
        
        self.sce_tasks = ETLStageCheckExchangeTasks()

    # Helpers

    def _test_task(self, task):
        self.task = task
        self.TEST_TASK_ID = self.task.task_id

        dagrun = self._create_dagrun()

        ti = dagrun.get_task_instance(task_id=self.TEST_TASK_ID)
        ti.task = self.dag.get_task(task_id=self.TEST_TASK_ID)
        ti.run(ignore_ti_state=True)
        self.assertEqual(TaskInstanceState.SUCCESS, ti.state)

    def _create_dagrun(self):
        """
        Must not be called until after having added task to dag.
        """
        return self.dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=self.DATA_INTERVAL_START,
            data_interval=(self.DATA_INTERVAL_START, self.DATA_INTERVAL_END),
            start_date=self.DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

    def _setup_test_dag(self):
        #self.DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
        now = datetime.datetime.now()
        # provide low granularity time to prevent:
        # sqlite3.IntegrityError: UNIQUE constraint failed: dag_run.dag_id, dag_run.run_id
        # do now.year-1 to ensure that Airflow won't complain about you giving a date in the future
        self.DATA_INTERVAL_START = pendulum.datetime(now.year-1, now.month, now.day, now.hour, now.minute, now.second, tz="UTC")
        self.DATA_INTERVAL_END = self.DATA_INTERVAL_START + datetime.timedelta(days=1)

        self.TEST_DAG_ID = "my_custom_operator_dag"

        self.dag = DAG(
            dag_id=self.TEST_DAG_ID,
            #schedule_interval="@daily",
            start_date=self.DATA_INTERVAL_START
        )

    def _setup_airflow_spark_connection(self):
        """
        Assumes that there is a spark cluster running at localhost.
        """
        # Mocking variables and connections
        # see https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#mocking-variables-and-connections
        conn = Connection(
            conn_type="spark",
            host="spark://127.0.0.1",
            port="7077"
        )
        conn_uri = conn.get_uri()
        #with mock.patch.dict("os.environ", AIRFLOW_CONN_SPARK=conn_uri):
        #    assert "127.0.0.1" == Connection.get("spark").host
        os.environ['AIRFLOW_CONN_SPARK'] = conn_uri
        # Reading Apache Airflow active connections programatically
        # see https://stackoverflow.com/questions/67492693/reading-apache-airflow-active-connections-programatically
        print('spark connection', conn_uri)
        #assert "7077" == BaseHook.get_connection('spark').port

    def _mock_airflow_variables(self):
        ETLSceTestBase._mock_airflow_variable(
            FileBase.get_datalake_root_key(),
            self.get_datalake_root()
        )
        ETLSceTestBase._mock_airflow_variable(
            FileBase.get_staging_root_key(),
            self.get_staging_root()
        )

    @staticmethod
    def _mock_airflow_variable(airflow_key, value):
        key = f'AIRFLOW_VAR_{airflow_key}'
        os.environ[key] = value
        # For the sake of creating test datasets:
        os.environ[airflow_key] = value

    @staticmethod
    def get_datalake_root():
        #return '/tmp/datalake'
        return 's3a://de-capstone-2022/datalake_test'

    @staticmethod
    def get_staging_root():
        return 'hdfs://localhost:9000/staging_test'