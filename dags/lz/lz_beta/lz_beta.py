"""DAG for loading Beta System Instance data to Landing Zone"""

import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator


from commons.configs import beta_instances
from lz.lz_beta import lz_beta_public


def _init_dag(instance):
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id=f'{lz_beta_public.DAG_ID}.{instance}',
        default_args=default_args,
        description='This DAG is replicates data from the source systems to Replica Hub',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_latest_only_op(dag):
    return LatestOnlyOperator(
        task_id='latest_only',
        dag=dag,
    )


def _create_deals_op(dag):
    return BashOperator(
        task_id=lz_beta_public.TaskId.DEALS,
        bash_command='echo "Load Deals for {{ ds }}"',
        dag=dag,
    )


def _create_dag(instance):
    # Init dag
    dag = _init_dag(instance)
    # Configure all operators
    latest_only_op = _create_latest_only_op(dag)
    deals_op = _create_deals_op(dag)
    # Setup dependencies
    latest_only_op >> [
        deals_op,
    ]
    return dag


def create_dags():
    for instance in beta_instances.Instance:
        dag = _create_dag(instance)
        globals()[dag.dag_id] = dag


create_dags()
