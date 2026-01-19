"""Type definitions for SquirrelDB client"""

from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime


@dataclass
class Document:
    """A document stored in SquirrelDB"""
    id: str
    collection: str
    data: dict[str, Any]
    created_at: str
    updated_at: str

    @classmethod
    def from_dict(cls, d: dict) -> "Document":
        return cls(
            id=d["id"],
            collection=d["collection"],
            data=d["data"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
        )


@dataclass
class ChangeEvent:
    """A change event from a subscription"""
    type: str  # "initial", "insert", "update", "delete"
    document: Optional[Document] = None
    new: Optional[Document] = None
    old: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, d: dict) -> "ChangeEvent":
        event_type = d["type"]
        if event_type == "initial":
            return cls(type=event_type, document=Document.from_dict(d["document"]))
        elif event_type == "insert":
            return cls(type=event_type, new=Document.from_dict(d["new"]))
        elif event_type == "update":
            return cls(type=event_type, old=d["old"], new=Document.from_dict(d["new"]))
        elif event_type == "delete":
            return cls(type=event_type, old=Document.from_dict(d["old"]))
        return cls(type=event_type)
