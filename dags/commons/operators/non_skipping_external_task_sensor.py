from airflow.sensors.external_task_sensor import ExternalTaskSensor


class NonSkippingExternalTaskSensor(ExternalTaskSensor):
    """External Task Sensor that does not skip downstream tasks

    Behaves like ExternalTaskSensor, except when parameterized with
    soft_fail=True it does not propagate the SKIPPED state
    to downstream tasks
    """

    def _do_skip_downstream_tasks(self, context):
        self.log.debug("Downstream tasks are not skipped in NotSkippingExternalTaskSensor")
