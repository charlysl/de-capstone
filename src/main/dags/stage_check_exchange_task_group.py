"""
An implementation of the Stack-Check-Exchange ETL pattern.

see: https://airflow.apache.org/docs/apache-airflow/1.10.2/concepts.html?highlight=branch%20operator
see: https://medium.com/@rchang/a-beginners-guide-to-data-engineering-part-ii-47c4e7cbda71
"""

from airflow.utils.task_group import TaskGroup

from operators.etl_spark_operator import ETLSparkOperator
from operators.etl_load_operator import ETLLoadOperator
from operators.etl_check_base_operator import ETLCheckBaseOperator

from datalake.model.file_base import FileBase


def create_task_group(transformation, output, dag=None, **kwargs):
    """
    Description: Create and compose all tasks required to create,
                 validate and load the transformation output data set.

    Parameters:
    - transformation: string - Transformation name, identifies a script
                     in the ```etl_spark_jobs``` directory.
    - output: FileBase - The dataset output by the transformation, 
                     it is the data set class in the 
                     ```datalake.datamodel.files``` module.

    Effects:
    - If the output data set passes all checks: the output data set is loaded.
    - Otherwise, the task fails, and the ouput data set is left in the 
      staging area.


    ### Design notes
    
    - The class associated with the **output data set** specifies
    which checks to perform and the loading destination.
    It would have been easy to parse the transformation script,
    as is done by some ELT systems, to automatically bring notebooks
    into production, to extract the output data set(s), but I decided
    to be more explicit, for clarity.

    - The **validation checks** are each performed in their own task, and,
    hence, its own spark job, in parallel, for better performance,
    even though it would have been easier to implement them in one
    spark job.
    """
    group_id = _get_group_id(transformation)
    with TaskGroup(group_id, prefix_group_id=False) as task_group:
        transformation_task = ETLSparkOperator(name=transformation)
        validation_tasks = _create_validation_tasks(output)
        #TODO replace with ETLLoadOperator, only needs 'file' arg
        load_task = ETLLoadOperator(
            name=f'load_{_get_output_name(output)}', file=output.__name__
        )
        _compose_tasks(transformation_task, validation_tasks, load_task)
    return task_group

def _create_validation_tasks(output):
    check_group_id = _get_check_group_id(output)
    with TaskGroup(
        group_id=check_group_id,
        prefix_group_id=False
    ) as check_group:
        tasks = [
            ETLCheckBaseOperator(name=_get_task_name(check), **check)
            for check in output().get_checks()
        ]
        return tasks

def _get_group_id(transformation):
    return f'{transformation}_group'

def _compose_tasks(transformation_task, validation_tasks, load_task):
    if len(validation_tasks) > 0:
        for validation_task in validation_tasks:
            transformation_task >> validation_task
            validation_task >> load_task
    else:
        transformation_task >> load_task

def _get_task_name(check):
    pfx = f"{check['table'][0]}_{check['check']}"
    if 'column' in check:
        if type(check['column']) == list:
            return f"{pfx}_{'_'.join(check['column'])}"
        else:
            return f"{pfx}_{check['column']}"
    else:
        return pfx

def _get_check_group_id(output):
    return f'{_get_output_name(output)}_validation_group'

def _get_output_name(output):
    return output().name