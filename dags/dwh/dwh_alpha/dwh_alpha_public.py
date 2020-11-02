"""Public constants for Alpha System data from LZ to DWH DAG"""

from enum import Enum

DAG_ID = 'dwh_alpha'


class TaskId(str, Enum):
    BALANCES = 'balances'
    BALANCES_INCREMENT = 'balances_increment'
    ACCOUNTS = 'accounts'

    def __str__(self):
        return self.value