# datalathe-client-python

Python client library for the [Datalathe](https://datalathe.com) API.

## Installation

```bash
pip install datalathe
```

Or install from source:

```bash
git clone https://github.com/DataLathe/datalathe-client-python.git
cd datalathe-client-python
pip install .
```

## Quick Start

```python
from datalathe import DatalatheClient, DatalatheResultSet

client = DatalatheClient("http://localhost:3000")

# Create a chip from a MySQL source
chip_id = client.create_chip("my_database", "SELECT * FROM users", "users")

# Query the chip
report = client.generate_report([chip_id], ["SELECT count(*) as total FROM users"])

# Iterate over results
rs = DatalatheResultSet(report.results[0])
for row in rs:
    print(row)  # {"total": 42}
```

## Creating Chips

### From MySQL

```python
chip_id = client.create_chip("my_database", "SELECT * FROM orders", "orders")
```

### From a file

```python
chip_id = client.create_chip_from_file("/data/sales.csv", "sales")
chip_id = client.create_chip_from_file("/data/events.parquet", "events")
```

### From existing chips

```python
chip_id = client.create_chip_from_chip(
    source_chip_ids=["chip-abc", "chip-def"],
    query="SELECT a.*, b.total FROM chip_abc a JOIN chip_def b ON a.id = b.id",
    table_name="joined",
)
```

### With partitions

```python
from datalathe import Partition

chip_id = client.create_chip(
    "my_database",
    "SELECT * FROM orders WHERE region = ?",
    "orders",
    partition=Partition(
        partition_by="region",
        partition_values=["US", "EU", "APAC"],
    ),
)
```

### With S3 storage

```python
from datalathe import S3StorageConfig

chip_id = client.create_chip(
    "my_database",
    "SELECT * FROM orders",
    "orders",
    storage_config=S3StorageConfig(bucket="my-bucket", key_prefix="chips/", ttl_days=30),
)
```

### Batch creation

```python
from datalathe import SourceRequest, SourceType

chip_ids = client.create_chips(
    sources=[
        SourceRequest(database_name="db", query="SELECT * FROM users", table_name="users"),
        SourceRequest(database_name="db", query="SELECT * FROM orders", table_name="orders"),
    ],
    source_type=SourceType.MYSQL,
    tags={"env": "production", "team": "analytics"},
)
```

## Querying

```python
report = client.generate_report(
    chip_ids=["chip-abc"],
    queries=[
        "SELECT count(*) as total FROM users",
        "SELECT status, count(*) as cnt FROM users GROUP BY status",
    ],
)

# Access results by query index
for idx, entry in report.results.items():
    print(f"Query {idx}: {entry.result}")

# Timing info
if report.timing:
    print(f"Total: {report.timing.total_ms}ms")
```

## Working with Results

`DatalatheResultSet` provides a cursor-based API for navigating query results.

```python
rs = DatalatheResultSet(report.results[0])

# Cursor-based iteration
while rs.next():
    name = rs.get_string("name")
    age = rs.get_int("age")
    score = rs.get_float("score")
    active = rs.get_boolean("active")
    print(f"{name}, {age}, {score}, {active}")

# Or iterate directly
for row in rs:
    print(row)

# Convert to list of dicts
rows = rs.to_list()

# Column metadata
print(rs.get_column_count())
print(rs.get_column_name(1))
print(rs.get_column_type(1))
```

## Chip Management

```python
# List all chips
response = client.list_chips()
for chip in response.chips:
    print(f"{chip.chip_id}: {chip.table_name}")

# Search chips
response = client.search_chips(table_name="users")
response = client.search_chips(tag="env:production")

# Tag a chip
client.add_chip_tags("chip-abc", {"env": "staging", "owner": "data-team"})
client.delete_chip_tag("chip-abc", "owner")

# Delete a chip
client.delete_chip("chip-abc")
```

## Chip Resolution

`ChipResolver` automates the find-or-create chip workflow. Register table
definitions once, then resolve chips or run full queries without manually
managing chip lifecycle.

```python
from datalathe import DatalatheClient, ChipResolver, TableDef

client = DatalatheClient("http://localhost:3000")

resolver = ChipResolver(client, table_defs=[
    TableDef("users", "select * from users", tenant_field="org_id"),
    TableDef("orders", "select * from orders",
             partitioned=True, partition_field="order_date",
             tenant_field="org_id"),
    TableDef("categories", "select * from categories"),
])

# Low-level: resolve chip IDs (finds cached chips, creates missing ones)
chip_ids = resolver.resolve_chips(
    tables=["users", "orders"],
    partition_values=["2024-01-31"],
    tenant_id="42",
)

# High-level: extract tables + resolve chips + run report in one call
result = resolver.query(
    sql="SELECT u.name, count(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    tenant_id="42",
    partition_values=["2024-01-31"],
)
```

The `query()` method automatically retries once on `ChipNotFoundError` (expired
chips). Disable with `retry_on_expired=False`.

## SQL Analysis

```python
# Extract table names from a query
tables = client.extract_tables("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
# ["users", "orders"]

# Extract tables and transform MySQL syntax to DuckDB
result = client.extract_tables_with_transform(
    "SELECT DATE_FORMAT(created_at, '%Y-%m') FROM users",
    transform=True,
)
print(result["tables"])
print(result["transformed_query"])
```

## Client Configuration

```python
client = DatalatheClient(
    base_url="http://localhost:3000",
    headers={"Authorization": "Bearer token"},
    timeout=60.0,  # seconds (default: 30)
)
```

## Error Handling

```python
from datalathe import DatalatheApiError, DatalatheStageError, ChipNotFoundError

try:
    chip_id = client.create_chip("bad_db", "SELECT 1", "test")
except DatalatheStageError as e:
    print(f"Staging failed: {e}")
except DatalatheApiError as e:
    print(f"API error {e.status_code}: {e.response_body}")

# ChipNotFoundError is raised when a referenced chip has expired or been deleted.
# ChipResolver.query() retries it automatically; handle it directly when
# calling generate_report() with cached chip IDs.
try:
    report = client.generate_report(["chip-abc"], ["SELECT count(*) FROM users"])
except ChipNotFoundError as e:
    print(f"Chip {e.chip_id} no longer exists, recreate it")
```

## Requirements

- Python 3.10+
- `requests` >= 2.28

## License

MIT
