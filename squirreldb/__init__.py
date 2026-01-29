"""SquirrelDB Python Client"""

from .client import SquirrelDB, connect
from .tcp import SquirrelDBTcp, SquirrelDBTcpSync, Subscription
from .types import Document, ChangeEvent
from .query import (
    QueryBuilder,
    table,
    field,
    and_,
    or_,
    not_,
    Field,
    Doc,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Contains,
    StartsWith,
    EndsWith,
    Exists,
)
from .storage import Storage, Bucket, Object, MultipartUpload, UploadPart
from .cache import Cache, CacheError

__all__ = [
    # Client
    "SquirrelDB",
    "connect",
    "SquirrelDBTcp",
    "SquirrelDBTcpSync",
    "Subscription",
    # Types
    "Document",
    "ChangeEvent",
    # Query Builder
    "QueryBuilder",
    "table",
    "field",
    "and_",
    "or_",
    "not_",
    "Field",
    "Doc",
    # Filter Operators
    "Eq",
    "Ne",
    "Gt",
    "Gte",
    "Lt",
    "Lte",
    "In",
    "NotIn",
    "Contains",
    "StartsWith",
    "EndsWith",
    "Exists",
    # Storage
    "Storage",
    "Bucket",
    "Object",
    "MultipartUpload",
    "UploadPart",
    # Cache
    "Cache",
    "CacheError",
]
__version__ = "0.1.0"
