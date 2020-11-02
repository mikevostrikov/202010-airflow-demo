"""DAG for loading Beta System data from Landing Zone to DWH

The DAG contains dummy steps, summarizing the state of corresponding
steps in DAGs, loading every Beta System Instance data from
Landing Zone to DWH
"""

import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from commons.configs import beta_instances
from dwh.dwh_beta import dwh_beta_public
from dwh.dwh_beta import dwh_beta_all_public


def _init_dag():
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id=dwh_beta_all_public.DAG_ID,
        default_args=default_args,
        description='Dummy tasks meaning that all instances are loaded',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_deals_dwh_op(dag, instance):
    external_dag_id = '{}.{}'.format(dwh_beta_public.DAG_ID, instance)
    external_task_id = dwh_beta_public.TaskId.DEALS
    return ExternalTaskSensor(
        task_id='{}.{}'.format(external_dag_id, external_task_id),
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=['success', 'skipped'],
        mode="reschedule",
        dag=dag
    )


def _create_deals_all_op(dag):
    return BashOperator(
        task_id=dwh_beta_all_public.TaskId.DEALS,
        bash_command='echo "All deals from Beta are loaded for {{ ds }}"',
        dag=dag,
    )


def _create_dag():
    dag = _init_dag()
    # Configure all operators
    deals_all_op = _create_deals_all_op(dag)
    for instance in beta_instances.Instance:
        deals_dwh_op = _create_deals_dwh_op(dag, instance)
        deals_dwh_op >> deals_all_op
    globals()[dag.dag_id] = dag


_create_dag()
