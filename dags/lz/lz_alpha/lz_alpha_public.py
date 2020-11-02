"""Public constants for Alpha System data to LZ DAG"""

from enum import Enum

DAG_ID = 'lz_alpha'


class TaskId(str, Enum):
    BALANCES = 'balances'
    BALANCES_INCREMENT = 'balances_increment'
    ACCOUNTS = 'accounts'

    def __str__(self):
        return self.value