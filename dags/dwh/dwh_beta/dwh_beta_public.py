"""Public constants for Beta System Instance data from LZ to DWH DAG"""

from enum import Enum

DAG_ID = 'dwh_beta'


class TaskId(str, Enum):
    DEALS = 'deals'

    def __str__(self):
        return self.value
