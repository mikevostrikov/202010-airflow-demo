"""DAG for Financial Data Mart ETL"""

import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dwh.dwh_alpha import dwh_alpha_public
from dwh.dwh_beta import dwh_beta_all_public
from dm.dm_finance import dm_finance_public
from pendulum import pendulum


def _init_dag():
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id=dm_finance_public.DAG_ID,
        default_args=default_args,
        description='Loads the Financial Data Mart',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_accounts_dwh_op(dag):
    external_dag_id = dwh_alpha_public.DAG_ID
    external_task_id = dwh_alpha_public.TaskId.ACCOUNTS
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_balances_dwh_op(dag):
    external_dag_id = dwh_alpha_public.DAG_ID
    external_task_id = dwh_alpha_public.TaskId.BALANCES
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_balances_increment_dwh_op(dag):
    external_dag_id = dwh_alpha_public.DAG_ID
    external_task_id = dwh_alpha_public.TaskId.BALANCES_INCREMENT
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_deals_dwh_op(dag):
    external_dag_id = dwh_beta_all_public.DAG_ID
    external_task_id = dwh_beta_all_public.TaskId.DEALS
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_daily_report_op(dag):
    return BashOperator(
        task_id=dm_finance_public.TaskId.DAILY_REPORT,
        bash_command='echo "Prepare daily report for {{ ds }}"',
        depends_on_past=True,
        dag=dag,
    )


def _wednesday(execution_date: pendulum.Pendulum, **kwargs):
    if not execution_date.is_wednesday():
        raise AirflowSkipException()


def _create_wednesday_op(dag):
    return PythonOperator(
        task_id='wednesday',
        python_callable=_wednesday,
        provide_context=True,
        dag=dag,
    )


def _create_weekly_report_op(dag):
    return BashOperator(
        task_id=dm_finance_public.TaskId.WEEKLY_REPORT,
        bash_command='echo "Prepare weekly report for {{ ds }}"',
        depends_on_past=True,
        dag=dag,
    )


def create_dag():
    # Init dag
    dag = _init_dag()
    # Configure all operators
    balances_increment_dwh_op = _create_balances_increment_dwh_op(dag)
    accounts_dwh_op = _create_accounts_dwh_op(dag)
    balances_dwh_op = _create_balances_dwh_op(dag)
    deals_dwh_op = _create_deals_dwh_op(dag)
    daily_report_op = _create_daily_report_op(dag)
    wednesday_op = _create_wednesday_op(dag)
    weekly_report_op = _create_weekly_report_op(dag)
    # Setup dependencies
    balances_increment_dwh_op >> accounts_dwh_op >> balances_dwh_op >> deals_dwh_op >> daily_report_op
    deals_dwh_op >> wednesday_op >> weekly_report_op
    globals()[dag.dag_id] = dag


create_dag()
