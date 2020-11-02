"""DAG for loading Beta System Instance data from Landing Zone to DWH"""

import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from commons.configs import beta_instances
from lz.lz_beta import lz_beta_public
from dwh.dwh_beta import dwh_beta_public
from dwh.dwh_alpha import dwh_alpha_public


def _init_dag(instance):
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id='{}.{}'.format(dwh_beta_public.DAG_ID, instance),
        default_args=default_args,
        description='Loads the Data Warehouse from Beta System Replica',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_deals_hub_op(dag, instance):
    external_dag_id = '{}.{}'.format(lz_beta_public.DAG_ID, instance)
    external_task_id = lz_beta_public.TaskId.DEALS
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


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


def _create_deals_op(dag):
    return BashOperator(
        task_id=dwh_beta_public.TaskId.DEALS,
        bash_command='echo "Load Deals for {{ ds }}"',
        depends_on_past=True,
        dag=dag,
    )


def _create_dag(instance):
    dag = _init_dag(instance)
    # Configure all operators
    deals_hub_op = _create_deals_hub_op(dag, instance)
    accounts_dwh_op = _create_accounts_dwh_op(dag)
    deals_op = _create_deals_op(dag)
    # Setup dependencies
    deals_hub_op >> accounts_dwh_op >> deals_op
    return dag


def create_dags():
    for instance in beta_instances.Instance:
        dag = _create_dag(instance)
        globals()[dag.dag_id] = dag


create_dags()
