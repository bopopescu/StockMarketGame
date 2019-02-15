from enum import Enum


class SMOrderTypes(Enum):
    Market = 1
    Limit = 2

    def __str__(self):
        return '%s' % self._value_
