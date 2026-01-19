#!/usr/bin/env python3
"""Basic example demonstrating SquirrelDB Python TCP SDK usage."""

import asyncio
from squirreldb import SquirrelDBTcp


async def main():
    # Connect to SquirrelDB server via TCP
    client = await SquirrelDBTcp.connect("localhost", 8082)
    print(f"Connected! Session ID: {client.session_id}")

    # Ping the server
    await client.ping()
    print("Ping successful!")

    # List collections
    collections = await client.list_collections()
    print(f"Collections: {collections}")

    # Insert a document
    doc = await client.insert("users", {
        "name": "Alice",
        "email": "alice@example.com",
        "active": True,
    })
    print(f"Inserted document: {doc}")

    # Query documents
    users = await client.query_raw('db.table("users").filter(u => u.active).run()')
    print(f"Active users: {users}")

    # Update the document
    updated = await client.update("users", doc.id, {
        "name": "Alice Updated",
        "email": "alice.updated@example.com",
        "active": True,
    })
    print(f"Updated document: {updated}")

    # Subscribe to changes
    print("\nSubscribing to user changes...")
    print("(Insert/update/delete users from another client to see changes)")
    print("Press Ctrl+C to exit.\n")

    try:
        async for change in await client.subscribe('db.table("users").changes()'):
            if change.type == "initial":
                print(f"Initial: {change.document}")
            elif change.type == "insert":
                print(f"Insert: {change.new}")
            elif change.type == "update":
                print(f"Update: {change.old} -> {change.new}")
            elif change.type == "delete":
                print(f"Delete: {change.old}")
    except KeyboardInterrupt:
        pass
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
