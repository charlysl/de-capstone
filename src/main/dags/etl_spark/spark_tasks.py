from airflow.models.variable import Variable

from datalake.utils.aws_helper import AwsHelper

from dags.etl_spark import spark_standalone
from dags.etl_spark.emr_tasks import EmrTasks
from etl_stage_check_exchange.tasks import ETLStageCheckExchangeTasks


sce_tasks = ETLStageCheckExchangeTasks()


def start_spark():
    """
    Description: start a Spark cluster for ETL.

    Effects:
    - if env var DE_CAPSTONE_IS_EMR == 'true': create and EMR Spark cluster
    - otherwise, start a local Spark cluster
    """
    if AwsHelper.is_running_in_emr():
        datalake_root = Variable.get('datalake_root')
        return EmrTasks.start_emr_cluster(datalake_root)
    else:
        return spark_standalone.start_spark()

def stop_spark():
    """
    Description: stop the Spark ETL cluster.

    Effects:
    - if env var DE_CAPSTONE_IS_EMR == 'true': destroy the ETL EMR Spark cluster
    - otherwise, stop the local ETL Spark cluster
    """
    if AwsHelper.is_running_in_emr():
        return EmrTasks.stop_emr_cluster()
    else:
        return spark_standalone.stop_spark()

def wait_for_spark_ready():
    """
    Description: Waits for Spark cluster to be ready for steps to be added.
    """
    return sce_tasks.stage(
        name='wait_for_spark_ready',
        application='noop'
    )