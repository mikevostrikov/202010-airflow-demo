"""DAG for loading Alpha System data from Landing Zone to DWH"""

import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dwh.dwh_alpha import dwh_alpha_public
from lz.lz_alpha import lz_alpha_public


def _init_dag():
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id=dwh_alpha_public.DAG_ID,
        default_args=default_args,
        description='Loads the Data Warehouse from Alpha System Replica',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_accounts_hub_op(dag):
    external_dag_id = lz_alpha_public.DAG_ID
    external_task_id = lz_alpha_public.TaskId.ACCOUNTS
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_accounts_op(dag):
    return BashOperator(
        task_id=lz_alpha_public.TaskId.ACCOUNTS,
        bash_command='echo "Load Accounts for {{ ds }}"',
        depends_on_past=True,
        dag=dag,
    )


def _create_balances_hub_op(dag):
    external_dag_id = lz_alpha_public.DAG_ID
    external_task_id = lz_alpha_public.TaskId.BALANCES
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_balances_increment_hub_op(dag):
    external_dag_id = lz_alpha_public.DAG_ID
    external_task_id = lz_alpha_public.TaskId.BALANCES_INCREMENT
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_balances_previous_day_op(dag):
    external_dag_id = dwh_alpha_public.DAG_ID
    external_task_id = lz_alpha_public.TaskId.BALANCES
    return ExternalTaskSensor(
        task_id=f'{external_task_id}-previous_day',
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        execution_delta=datetime.timedelta(days=1),
        mode="reschedule",
        dag=dag
    )


def _create_balances_increment_op(dag):
    return BashOperator(
        task_id=lz_alpha_public.TaskId.BALANCES_INCREMENT,
        bash_command='echo "Load Balances Increment for {{ ds }}"',
        depends_on_past=True,
        dag=dag,
    )


def _create_balances_op(dag):
    return BashOperator(
        task_id=lz_alpha_public.TaskId.BALANCES,
        bash_command='echo "Load Balances for {{ ds }}"',
        dag=dag,
    )


def create_dag():
    # Init dag
    dag = _init_dag()
    # Configure all operators
    accounts_hub_op = _create_accounts_hub_op(dag)
    accounts_op = _create_accounts_op(dag)
    balances_hub_op = _create_balances_hub_op(dag)
    balances_op = _create_balances_op(dag)
    balances_previous_day_op = _create_balances_previous_day_op(dag)
    balances_increment_hub_op = _create_balances_increment_hub_op(dag)
    balances_increment_op = _create_balances_increment_op(dag)
    # Setup dependencies
    accounts_hub_op >> accounts_op
    [balances_hub_op, accounts_op] >> balances_op
    balances_previous_day_op >> balances_increment_hub_op >> balances_increment_op
    globals()[dag.dag_id] = dag


create_dag()
