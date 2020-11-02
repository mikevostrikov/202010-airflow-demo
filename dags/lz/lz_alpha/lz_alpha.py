"""DAG for loading Alpha System data to Landing Zone"""

import datetime
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.executors import get_default_executor
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.db import provide_session
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import func

from commons.operators.soft_fail_operator import SoftFailOperator
import commons.configs.balance_increment
from lz.lz_alpha import lz_alpha_public


def _init_dag():
    default_args = {
        'owner': 'airflow',
    }
    dag = DAG(
        dag_id=lz_alpha_public.DAG_ID,
        default_args=default_args,
        description='Replicates data from Alpha System to Replica Hub',
        schedule_interval='@daily',
        start_date=datetime.datetime(2020, 10, 1),
    )
    return dag


def _create_balances_op(dag):
    return BashOperator(
        task_id=lz_alpha_public.TaskId.BALANCES,
        bash_command='echo "Load Balances for {{ ds }}"',
        dag=dag,
    )


# noinspection PyUnusedLocal
def _balances_increment_check_within_period(execution_date: pendulum.Pendulum,
                                            balance_date: pendulum.Date,
                                            days_ago: int,
                                            **kwargs):
    execution_date_ = pendulum.Date.create(execution_date.year, execution_date.month, execution_date.day)
    diff = balance_date.diff(execution_date_, abs=False).in_days()
    skipping = not (0 < diff <= days_ago)
    print(f"execution_date={execution_date_}, balance_date={balance_date},"
          f"days_ago={days_ago}, diff={diff}, skipping={skipping}")
    if skipping:
        raise AirflowSkipException()


def _create_balances_increment_check_within_period_op(dag, balance_date: pendulum.Date, days_ago: int):
    return PythonOperator(
        task_id=f"execution_date_is_within_next_{days_ago}_days_after_{balance_date.to_date_string()}",
        python_callable=_balances_increment_check_within_period,
        provide_context=True,
        op_kwargs={
            'balance_date': balance_date,
            'days_ago': days_ago,
        },
        dag=dag
    )


@provide_session
def _check_balances_loaded(balance_date: pendulum.Date, session=None):
    TI = TaskInstance
    count = session.query(func.count()).filter(
        TI.dag_id == lz_alpha_public.DAG_ID,
        TI.task_id == lz_alpha_public.TaskId.BALANCES,
        TI.state.in_(['success', 'skipped']),
        TI.execution_date == pendulum.Pendulum.create(balance_date.year, balance_date.month, balance_date.day),
    ).scalar()
    print(f"count={count}")
    if count == 0:
        raise AirflowFailException()


def _create_check_balances_loaded_op(dag, balance_date: pendulum.Date):
    return PythonOperator(
        task_id=f"balances_are_loaded_for_{balance_date.to_date_string()}",
        python_callable=_check_balances_loaded,
        op_kwargs={
            'balance_date': balance_date,
        },
        dag=dag,
    )


def _create_soft_fail_op(dag, task_id, balance_date):
    return SoftFailOperator(
        task_id=f'skip_if_previous_task_is_failed_for_{balance_date.to_date_string()}',
        upstream_task_id=task_id,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )


def _create_detect_delta_op(dag, balance_date: pendulum.Date):
    return BashOperator(
        task_id=f'detect_delta_for_{balance_date.to_date_string()}',
        bash_command=f'echo "Detect Balances delta for {balance_date.to_date_string()}"',
        dag=dag,
    )


def _create_apply_delta_op(dag, balance_date: pendulum.Date):
    return BashOperator(
        task_id=f'apply_delta_for_{balance_date.to_date_string()}',
        bash_command=f'echo "Apply Balances delta for {balance_date.to_date_string()}"',
        dag=dag,
    )


def _create_balances_increment_op(dag):
    subdag = DAG(
        dag_id='%s.%s' % (dag.dag_id, 'balances_increment'),
        default_args=dag.default_args,
        schedule_interval=dag.schedule_interval,
        start_date=dag.start_date,
    )
    balances_increment_op = SubDagOperator(
        task_id='balances_increment',
        subdag=subdag,
        executor=get_default_executor(),
        dag=dag,
    )
    join_op = DummyOperator(
        task_id="all_done_none_failed",
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=subdag
    )
    current_date: pendulum.Date = pendulum.Date.today()
    for i in range(1, commons.configs.balance_increment.NUM_DAYS_TO_MONITOR_BEFORE_TODAY + 1):
        balance_date: pendulum.Date = current_date.add(days=-i)
        balance_success_op = _create_check_balances_loaded_op(subdag, balance_date)
        soft_fail_op = _create_soft_fail_op(subdag, balance_success_op.task_id, balance_date)
        detect_delta_one_day_op = _create_detect_delta_op(subdag, balance_date)
        apply_delta_one_day_op = _create_apply_delta_op(subdag, balance_date)
        balances_increment_check_within_period_op = \
            _create_balances_increment_check_within_period_op(
                subdag,
                balance_date,
                commons.configs.balance_increment.NUM_DAYS_TO_UPDATE_AFTER_LOADING
            )
        balances_increment_check_within_period_op >> balance_success_op \
        >> soft_fail_op >> detect_delta_one_day_op >> apply_delta_one_day_op >> join_op
    return balances_increment_op


def _create_latest_only_op(dag):
    return LatestOnlyOperator(
        task_id='latest_only',
        dag=dag,
    )


def _create_accounts_op(dag):
    return BashOperator(
        task_id='accounts',
        bash_command='echo "Load Accounts for {{ ds }}"',
        dag=dag,
    )


def create_dag():
    # Init dag
    dag = _init_dag()
    # Configure all operators
    latest_only_op = _create_latest_only_op(dag)
    _create_balances_op(dag)
    _create_balances_increment_op(dag)
    accounts_op = _create_accounts_op(dag)
    # Setup dependencies
    latest_only_op >> [
        accounts_op,
    ]
    globals()[dag.dag_id] = dag


create_dag()
