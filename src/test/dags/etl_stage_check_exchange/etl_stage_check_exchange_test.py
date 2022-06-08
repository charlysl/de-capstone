import unittest
import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator

from dags.etl_stage_check_exchange.pattern import ETLStageCheckExchange

from datalake.model.file_base import FileBase


class ETLStageCheckExchangeTests(unittest.TestCase):
    """
    These are unit tests, not integration tests.

    The goal of each test is just to verify that each function
    behaves according to its specification, not to verify that
    the task group executes as expected.

    Hence, what is verified that the different tasks in the group
    are configured as expected, not that they execute as expected.
    """

    def setUp(self):
        super().setUp()

        # a dummy dataset class for testing purposes
        # manually set get_checks() inside each test as needed
        class Output(FileBase):
            def __init__(self):
                super().__init__('output', None, FileBase.production)

        self.output = Output

        # no actual dataset operations will be invoked during
        # any test, so just set a dummy DATALAKE and STAGING_ROOT
        # just to prevent exceptions form being thrown
        os.environ['DATALAKE_ROOT'] = 'dummy_datalake_root'
        os.environ['STAGING_ROOT'] = 'dummy_staging_root'

        # setup a dummy dag and operator, which, again, will
        # not ran by any test
        self.dag = DAG(
            dag_id='dummy_dag',
            start_date=datetime.now()
        )
        
        self.sce = ETLStageCheckExchange()


    # _get_group_id tests

    def test_get_group_id(self):
        actual = self.sce._get_group_id('some_transformation')
        expected = 'some_transformation_group'
        self.assertEqual(expected, actual)
    

    # _create_validation_tasks tests

    def test_create_validation_tasks_no_checks(self):
        actual = self.sce._create_validation_tasks(self.output)
        self.assertEqual(0, len(actual))
    
    def test_create_validation_tasks_one_check(self):
        self.output.get_checks = lambda _: [
            {'check': 'dummy_check', 'table': ['dummy_table']}
        ]
        with self.dag:
            actual = self.sce._create_validation_tasks(self.output)
        self.assertEqual(1, len(actual))
    
    def test_create_validation_tasks_two_checks(self):
        checks = [
            {'check': 'dummy_check1', 'table': ['dummy_table']},
            {'check': 'dummy_check2', 'table': ['dummy_table']}
        ]
        self.output.get_checks = lambda _: checks
        with self.dag:
            actual = self.sce._create_validation_tasks(self.output)
        self.assertEqual(2, len(actual))
    

    # _compose_tasks_no_checks tests

    def test_compose_tasks_no_checks(self):
        transformation_task = self._create_dummy_task('transformation_task')
        validation_tasks = []
        load_task = self._create_dummy_task('load_task')

        self.sce._compose_tasks(
            transformation_task, validation_tasks, load_task
        )

        self.assertIn(
            load_task.task_id,
            transformation_task._downstream_task_ids
        )
    
    def test_compose_tasks_one_check(self):
        transformation_task = self._create_dummy_task('transformation_task')
        validation_tasks = [self._create_dummy_task('check_task')]
        load_task = self._create_dummy_task('load_task')

        self.sce._compose_tasks(
            transformation_task, validation_tasks, load_task
        )

        self.assertIn(
            validation_tasks[0].task_id,
            transformation_task._downstream_task_ids
        )
        self.assertIn(
            load_task.task_id,
            validation_tasks[0]._downstream_task_ids
        )
    
    def test_compose_tasks_two_checks(self):
        transformation_task = self._create_dummy_task('transformation_task')
        validation_tasks = [
            self._create_dummy_task('check_taskA'),
            self._create_dummy_task('check_taskB')
        ]
        load_task = self._create_dummy_task('load_task')

        self.sce._compose_tasks(
            transformation_task, validation_tasks, load_task
        )

        for validation_task in validation_tasks:
            self.assertIn(
                validation_task.task_id,
                transformation_task._downstream_task_ids
            )
            self.assertIn(
                load_task.task_id,
                validation_task._downstream_task_ids
            )


    # _create_task_group tests

    def test_create_task_group_no_checks(self):
        group_name = 'dummy_transformation'
        with self.dag:
            task_group = self.sce.create(
                group_name, self.output
            )

        # assert that task group has expected id
        self.assertEqual(f'{group_name}_group', task_group._group_id)

        # assert that task group has expected number of tasks
        self.assertEqual(2, len(task_group.children))

        # assert that task group has all expected tasks
        for task in [f'{group_name}_task', 'load_output_task']:
            self.assertIn(
                f'{task}',
                task_group.children.keys()
            )

    def test_create_task_group_one_check(self):
        check = {'check': 'dummy_check', 'table': ['dummy_table']}
        self.output.get_checks = lambda _: [check]

        group_name = 'dummy_transformation'
        with self.dag:
            task_group = self.sce.create(
                group_name, self.output
            )

        # assert that task group has expected id
        self.assertEqual(f'{group_name}_group', task_group._group_id)

        # assert that task group has expected number of tasks
        self.assertEqual(3, len(task_group.children))

        # assert that task group has all expected tasks
        
        for task in [f'{group_name}_task', 'load_output_task']:
            self.assertIn(
                task,
                task_group.children.keys()
            )

        # assert that the task group has expected validation task
        self.assertIn(
            f"{check['table'][0]}_{check['check']}_task",
            task_group.children['output_validation_group'].children.keys()
        )


    def test_create_task_group_two_checks(self):

        checks = [
            {'check': 'dummy_checkA', 'table': ['dummy_tableA']},
            {'check': 'dummy_checkB', 'table': ['dummy_tableB']}
        ]

        self.output.get_checks = lambda _: checks

        group_name = 'dummy_transformation'
        with self.dag:
            task_group = self.sce.create(
                group_name, self.output
            )

        # assert that task group has expected id
        self.assertEqual(f'{group_name}_group', task_group._group_id)

        # assert that task group has expected number of tasks
        self.assertEqual(3, len(task_group.children))

        # assert that task group has all expected tasks
        expected_task_ids = [
            f'{group_name}_task', 'load_output_task'
        ]
        for task in expected_task_ids:
            self.assertIn(
                task,
                task_group.children.keys()
            )

        validation_group = task_group.children['output_validation_group']

        # assert that task group has expected number of validation tasks
        self.assertEqual(2, len(validation_group.children))

        # assert that the task group has expected validation task
        for check in checks:
            self.assertIn(
                f"{check['table'][0]}_{check['check']}_task",
                validation_group.children.keys()
            )

    # helpers

    def _create_dummy_task(self, id):
        return DummyOperator(
            task_id=id, dag=self.dag
        )

if __name__ == '__main__':
    unittest.main()