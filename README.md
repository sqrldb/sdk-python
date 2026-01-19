# SquirrelDB Python SDK

Official Python client for SquirrelDB.

## Installation

```bash
pip install squirreldb
```

## Quick Start

```python
from squirreldb import SquirrelDB
import os

db = SquirrelDB(
    host="localhost",
    port=8080,
    token=os.environ.get("SQUIRRELDB_TOKEN")
)
db.connect()

# Insert a document
user = db.table("users").insert({
    "name": "Alice",
    "email": "alice@example.com"
})
print(f"Created user: {user['id']}")

# Query documents
active_users = db.table("users").filter("u => u.status === 'active'").run()

# Subscribe to changes (async)
import asyncio

async def watch_messages():
    async for change in db.table("messages").changes():
        print(f"Change: {change['operation']} - {change['newValue']}")

asyncio.run(watch_messages())
```

## Documentation

Visit [squirreldb.com/docs/sdks](https://squirreldb.com/docs/sdks) for full documentation.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
