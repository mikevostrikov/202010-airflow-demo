"""Enumeration of instances of the Beta System"""

from enum import Enum


class Instance(str, Enum):
    NORTH = 'north'
    SOUTH = 'south'

    def __str__(self):
        return self.value


if __name__ == '__main__':
    for i in Instance:
        print(i)
