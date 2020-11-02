"""Public constants of the Financial Data Mart ETL DAG"""

from enum import Enum

DAG_ID = 'dm_finance'


class TaskId(str, Enum):
    DAILY_REPORT = 'daily_report'
    WEEKLY_REPORT = 'weekly_report'

    def __str__(self):
        return self.value