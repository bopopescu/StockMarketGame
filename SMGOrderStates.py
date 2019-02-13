from enum import Enum


class SMOrderStates(Enum):
    Open = 1
    Partial = 2
    Filled = 3
    PendingCancel = 4
    Cancelled = 5
    Rejected = 6
