"""SquirrelDB Python Client"""

from .client import SquirrelDB, connect
from .tcp import SquirrelDBTcp, SquirrelDBTcpSync, Subscription
from .types import Document, ChangeEvent

__all__ = [
    "SquirrelDB",
    "connect",
    "SquirrelDBTcp",
    "SquirrelDBTcpSync",
    "Subscription",
    "Document",
    "ChangeEvent",
]
__version__ = "0.1.0"
