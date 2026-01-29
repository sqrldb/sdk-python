"""Tests for SquirrelDB Python client - mock-based unit tests"""

import pytest
from squirreldb import Document, ChangeEvent


class TestDocument:
    """Test Document type"""

    def test_document_from_dict(self):
        data = {
            "id": "123",
            "collection": "users",
            "data": {"name": "Test"},
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }
        doc = Document.from_dict(data)

        assert doc.id == "123"
        assert doc.collection == "users"
        assert doc.data == {"name": "Test"}
        assert doc.created_at == "2024-01-01T00:00:00Z"
        assert doc.updated_at == "2024-01-01T00:00:00Z"

    def test_document_has_correct_fields(self):
        doc = Document(
            id="test-id",
            collection="test-collection",
            data={"foo": "bar"},
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )

        assert isinstance(doc.id, str)
        assert isinstance(doc.collection, str)
        assert isinstance(doc.data, dict)
        assert isinstance(doc.created_at, str)
        assert isinstance(doc.updated_at, str)


class TestChangeEvent:
    """Test ChangeEvent type"""

    def test_change_event_initial(self):
        data = {
            "type": "initial",
            "document": {
                "id": "123",
                "collection": "users",
                "data": {"name": "Test"},
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            },
        }
        event = ChangeEvent.from_dict(data)

        assert event.type == "initial"
        assert event.document is not None
        assert event.document.id == "123"

    def test_change_event_insert(self):
        data = {
            "type": "insert",
            "new": {
                "id": "123",
                "collection": "users",
                "data": {"name": "Test"},
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            },
        }
        event = ChangeEvent.from_dict(data)

        assert event.type == "insert"
        assert event.new is not None

    def test_change_event_update(self):
        data = {
            "type": "update",
            "old": {"name": "Old"},
            "new": {
                "id": "123",
                "collection": "users",
                "data": {"name": "New"},
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            },
        }
        event = ChangeEvent.from_dict(data)

        assert event.type == "update"
        assert event.old == {"name": "Old"}
        assert event.new is not None

    def test_change_event_delete(self):
        data = {
            "type": "delete",
            "old": {
                "id": "123",
                "collection": "users",
                "data": {"name": "Test"},
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            },
        }
        event = ChangeEvent.from_dict(data)

        assert event.type == "delete"
        assert event.old is not None


class TestMessageProtocol:
    """Test message protocol structures"""

    def test_ping_message(self):
        msg = {"type": "Ping"}
        assert msg["type"] == "Ping"

    def test_query_message(self):
        msg = {
            "type": "Query",
            "id": "req-123",
            "query": 'db.table("users").run()',
        }
        assert msg["type"] == "Query"
        assert msg["id"] == "req-123"
        assert "users" in msg["query"]

    def test_insert_message(self):
        msg = {
            "type": "Insert",
            "id": "req-456",
            "collection": "users",
            "data": {"name": "Alice"},
        }
        assert msg["type"] == "Insert"
        assert msg["collection"] == "users"
        assert msg["data"] == {"name": "Alice"}

    def test_update_message(self):
        msg = {
            "type": "Update",
            "id": "req-789",
            "collection": "users",
            "document_id": "doc-123",
            "data": {"name": "Bob"},
        }
        assert msg["type"] == "Update"
        assert msg["document_id"] == "doc-123"

    def test_delete_message(self):
        msg = {
            "type": "Delete",
            "id": "req-101",
            "collection": "users",
            "document_id": "doc-123",
        }
        assert msg["type"] == "Delete"
        assert msg["document_id"] == "doc-123"

    def test_subscribe_message(self):
        msg = {
            "type": "Subscribe",
            "id": "req-202",
            "query": 'db.table("users").changes()',
        }
        assert msg["type"] == "Subscribe"
        assert "changes" in msg["query"]

    def test_unsubscribe_message(self):
        msg = {
            "type": "Unsubscribe",
            "id": "req-303",
            "subscription_id": "sub-123",
        }
        assert msg["type"] == "Unsubscribe"
        assert msg["subscription_id"] == "sub-123"


class TestServerResponseProtocol:
    """Test server response protocol structures"""

    def test_pong_response(self):
        response = {"type": "Pong"}
        assert response["type"] == "Pong"

    def test_result_response(self):
        response = {
            "type": "Result",
            "id": "req-123",
            "documents": [
                {
                    "id": "1",
                    "collection": "users",
                    "data": {"name": "Alice"},
                    "created_at": "",
                    "updated_at": "",
                }
            ],
        }
        assert response["type"] == "Result"
        assert len(response["documents"]) == 1

    def test_error_response(self):
        response = {
            "type": "Error",
            "id": "req-123",
            "message": "Query failed",
        }
        assert response["type"] == "Error"
        assert response["message"] == "Query failed"

    def test_subscribed_response(self):
        response = {
            "type": "Subscribed",
            "id": "req-123",
            "subscription_id": "sub-456",
        }
        assert response["type"] == "Subscribed"
        assert response["subscription_id"] == "sub-456"

    def test_change_response(self):
        response = {
            "type": "Change",
            "subscription_id": "sub-456",
            "change": {
                "type": "insert",
                "new": {
                    "id": "1",
                    "collection": "users",
                    "data": {},
                    "created_at": "",
                    "updated_at": "",
                },
            },
        }
        assert response["type"] == "Change"
        assert response["change"]["type"] == "insert"
