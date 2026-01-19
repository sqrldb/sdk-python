"""Tests for SquirrelDB Python client"""

import os
import asyncio
import pytest
from squirreldb import SquirrelDB, Document, ChangeEvent

TEST_URL = os.environ.get("SQUIRRELDB_URL", "localhost:8080")


@pytest.fixture
async def db():
    """Create a connected client for testing"""
    client = await SquirrelDB.connect(TEST_URL)
    yield client
    await client.close()


class TestConnection:
    """Test connection functionality"""

    @pytest.mark.asyncio
    async def test_connect_without_prefix(self):
        db = await SquirrelDB.connect(TEST_URL)
        await db.ping()
        await db.close()

    @pytest.mark.asyncio
    async def test_connect_with_ws_prefix(self):
        db = await SquirrelDB.connect(f"ws://{TEST_URL}")
        await db.ping()
        await db.close()

    @pytest.mark.asyncio
    async def test_ping(self, db):
        await db.ping()  # Should not raise


class TestCRUD:
    """Test CRUD operations"""

    @pytest.mark.asyncio
    async def test_insert_document(self, db):
        doc = await db.insert("py_test_users", {"name": "Alice", "age": 30})

        assert isinstance(doc, Document)
        assert doc.id is not None
        assert doc.collection == "py_test_users"
        assert doc.data == {"name": "Alice", "age": 30}
        assert doc.created_at is not None
        assert doc.updated_at is not None

    @pytest.mark.asyncio
    async def test_query_documents(self, db):
        # Insert a document first
        await db.insert("py_test_query", {"name": "Bob", "age": 25})

        docs = await db.query('db.table("py_test_query").run()')

        assert isinstance(docs, list)
        assert len(docs) > 0
        assert all(isinstance(d, Document) for d in docs)

    @pytest.mark.asyncio
    async def test_update_document(self, db):
        inserted = await db.insert("py_test_update", {"name": "Charlie", "age": 35})
        updated = await db.update(
            "py_test_update", inserted.id, {"name": "Charlie", "age": 36}
        )

        assert updated.id == inserted.id
        assert updated.data == {"name": "Charlie", "age": 36}

    @pytest.mark.asyncio
    async def test_delete_document(self, db):
        inserted = await db.insert("py_test_delete", {"name": "Dave", "age": 40})
        deleted = await db.delete("py_test_delete", inserted.id)

        assert deleted.id == inserted.id

    @pytest.mark.asyncio
    async def test_list_collections(self, db):
        # Ensure at least one collection exists
        await db.insert("py_test_list", {"test": True})

        collections = await db.list_collections()

        assert isinstance(collections, list)
        assert len(collections) > 0
        assert all(isinstance(c, str) for c in collections)


class TestSubscriptions:
    """Test subscription functionality"""

    @pytest.mark.asyncio
    async def test_subscribe_and_unsubscribe(self, db):
        changes = []

        def on_change(change: ChangeEvent):
            changes.append(change)

        sub_id = await db.subscribe('db.table("py_test_sub").changes()', on_change)

        assert sub_id is not None
        assert isinstance(sub_id, str)

        # Insert a document to trigger a change
        await db.insert("py_test_sub", {"name": "Eve", "age": 28})

        # Wait for change to arrive
        await asyncio.sleep(0.1)

        # Unsubscribe
        await db.unsubscribe(sub_id)

        # Should have received at least one change
        assert len(changes) > 0
        assert all(isinstance(c, ChangeEvent) for c in changes)


class TestErrors:
    """Test error handling"""

    @pytest.mark.asyncio
    async def test_invalid_query_raises_exception(self, db):
        with pytest.raises(Exception):
            await db.query("invalid query syntax")


class TestTypes:
    """Test type structures"""

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
