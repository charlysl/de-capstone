from airflow.plugins_manager import AirflowPlugin
from operators.etl_spark_operator import ETLSparkOperator
from operators.etl_check_base_operator import ETLCheckBaseOperator

from operators.check_not_empty_operator import CheckNotEmptyOperator
from operators.check_no_nulls_operator import CheckNoNullsOperator
from operators.check_no_duplicates_operator import CheckNoDuplicatesOperator
from operators.check_referential_integrity_operator import CheckReferentialIntegrityOperator

class ETLValidationPlugin(AirflowPlugin):
    name='etl_validation_plugin'
    operators=[
        ETLSparkOperator,
        ETLCheckBaseOperator,
        CheckNotEmptyOperator,
        CheckNoNullsOperator,
        CheckNoDuplicatesOperator,
        CheckReferentialIntegrityOperator
    ]