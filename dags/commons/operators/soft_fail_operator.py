from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class SoftFailOperator(BaseOperator):
    """Softens immediately upstream failed operator/sensor

    When the immediately upstream operator/sensor is `failed` or `skipped`:
    this operator goes into `skipped` state

    When the immediately upstream operator/sensor is in `success` state:
    this operator goes into `success` state

    When the immediately upstream operator/sensor is in any other state:
    this operator goes into `failed` state

    This operator is supposed to be used with `all_done` trigger rule only
    """

    @apply_defaults
    def __init__(self, upstream_task_id, *args, **kwargs):
        super(SoftFailOperator, self).__init__(*args, **kwargs)
        self.upstream_task_id = upstream_task_id

    def execute(self, context):
        task = self.dag.get_task(self.upstream_task_id)
        ti = TaskInstance(task, context['execution_date'])
        if ti.current_state() == State.FAILED or ti.current_state() == State.SKIPPED:
            raise AirflowSkipException('Failed state leads to skipped')
        elif ti.current_state() != State.SUCCESS:
            raise AirflowException("Upstream task's status differs from: FAILED, SUCCESS, SKIPPED")
