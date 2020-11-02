"""Public constants for Beta System data to LZ DAG"""

from enum import Enum

DAG_ID = 'lz_beta'


class TaskId(str, Enum):
    DEALS = 'deals'

    def __str__(self):
        return self.value