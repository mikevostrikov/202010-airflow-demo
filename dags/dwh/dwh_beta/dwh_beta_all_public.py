"""Public constants for Beta System data from LZ to DWH DAG"""

from enum import Enum

DAG_ID = 'dwh_beta_all'


class TaskId(str, Enum):
    DEALS = 'deals'

    def __str__(self):
        return self.value