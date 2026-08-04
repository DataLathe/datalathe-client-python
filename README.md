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

### Async ingest

For large MySQL sources, `create_chip_async` submits the ingest as a background
job (engine 1.7.12+) and returns immediately with a job handle. Poll with
`get_ingest_job`, or block until completion with `wait_for_ingest`.

```python
job = client.create_chip_async("my_database", "SELECT * FROM orders", "orders")

# Poll manually
job = client.get_ingest_job(job.job_id)
print(job.status, job.rows_ingested)

# Or block until the job reaches a terminal state
job = client.wait_for_ingest(job.job_id, poll_interval=2.0, timeout=600.0)
print(job.chip_id)  # ready to query
```

`wait_for_ingest` raises `DatalatheIngestError` if the job ends failed or
cancelled, and `DatalatheIngestTimeoutError` if it does not finish within the
timeout. List jobs (optionally filtered by status) with `list_ingest_jobs`, and
restart a resumable failed job with `resume_ingest_job`:

```python
failed = client.list_ingest_jobs(status="failed")
job = client.resume_ingest_job(failed[0].job_id)
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

### Streaming results

For large results, `generate_report_stream` streams rows from the server instead
of buffering the whole result, and is not subject to the server's row cap
(`max_result_rows` applies to the buffered path only). It takes a single query
and returns a `DatalatheStreamingResultSet` — a forward-only cursor that pulls
rows lazily as you advance it. Use it as a context manager so the underlying
connection is always released:

```python
with client.generate_report_stream(["chip-abc"], ["SELECT * FROM events"]) as rs:
    for row in rs:
        print(row)
    print(rs.row_count)  # total rows, available once the stream is consumed
    print(rs.timing)     # server-side timing from the terminal frame
```

The streaming cursor mirrors the `DatalatheResultSet` accessor surface
(`next()`, `get_string()`, `get_int()`, iteration, schema access), but backward
navigation (`previous`, `first`, `last`, `absolute`) is unsupported, and
`row_count` is `None` until the stream is fully consumed. If you abandon the
cursor early (for example, breaking out of iteration outside a `with` block),
call `close()` to release the connection; it is idempotent.

### Raw chip queries

`query_chips` runs a single read-only SQL statement against the chips' raw
catalogs (engine 1.11+). Unlike report queries there is no view layer: the
statement sees every table inside the attached chips via
`s_<sub_chip_id>.main.<table>`, including staging leftovers. Results are
truncated at the engine's `max_result_rows` cap (`truncated` flag).

```python
result = client.query_chips(
    ["chip-abc"],
    "SELECT COUNT(*) AS n FROM s_chip_abc.main.loans",
)
print([c.name for c in result.columns])
print(result.rows)
print(result.truncated)
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

# Extract tables and transform MySQL syntax to the engine's SQL dialect
result = client.extract_tables_with_transform(
    "SELECT DATE_FORMAT(created_at, '%Y-%m') FROM users",
    transform=True,
)
print(result["tables"])
print(result["transformed_query"])
```

## Connection Management

Manage the engine's named MySQL connections. `upsert_connection` creates or
updates a connection under an alias; `test_connection` verifies it is reachable.

```python
resp = client.upsert_connection(
    alias="my_database",
    host="db.internal",
    port="3306",
    database="analytics",
    user="reader",
    password="secret",
)

resp = client.test_connection("my_database")
print(resp.status)  # or resp.error on failure

for conn in client.list_connections():
    print(f"{conn.alias}: {conn.user}@{conn.host}:{conn.port}/{conn.database}")
```

`get_connection(alias)` fetches a single connection and `delete_connection(alias)`
removes it.

## AI Agent

`query_agent` asks a natural-language question against a context chip and
returns the agent's answer along with its tool calls and any attachments.

```python
response = client.query_agent(
    context_id="context-chip-id",
    user_question="Which region had the highest order growth last quarter?",
)
print(response.answer)

# Continue the conversation in the same session
follow_up = client.query_agent(
    context_id="context-chip-id",
    user_question="Break that down by month.",
    session_id=response.session_id,
)
```

Optional parameters include `tenant_id` (scope the agent to a tenant's data),
`model`, `conversation_history`, and `agent_options` (an `AgentOptions` with
budget caps such as `max_iterations` and `max_tool_calls`).

## Client Configuration

```python
client = DatalatheClient(
    base_url="http://localhost:3000",
    headers={"Authorization": "Bearer token"},
    timeout=60.0,  # seconds (default: 30)
)
```

When the engine sheds load it returns HTTP 429 with a `Retry-After` header,
having done no work on the request, so the client transparently retries 429
responses for every method (up to 3 attempts by default, honoring
`Retry-After`). Network errors are never retried. Tune or disable with:

```python
client = DatalatheClient(
    base_url="http://localhost:3000",
    retry_on_429=False,  # default: True
    max_retries=5,       # 429 retry budget (default: 3)
)
```

If retries are exhausted, the final 429 surfaces as a normal
`DatalatheApiError`.

## Error Handling

```python
from datalathe import (
    DatalatheApiError,
    DatalatheStageError,
    ChipNotFoundError,
    DatalatheQueryError,
    DatalatheIngestError,
    DatalatheIngestTimeoutError,
)

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

# A query that fails at execution time (bad column, type mismatch, an
# untranslated function) comes back from the engine HTTP 200 with the error
# in the per-query result. generate_report() and ChipResolver.query() raise
# DatalatheQueryError on that by default so it is not mistaken for 0 rows.
try:
    report = client.generate_report(["chip-abc"], ["SELECT bogus FROM users"])
except DatalatheQueryError as e:
    print(f"Query failed: {e.errors}")  # {0: "Binder Error: ..."}

# Pass raise_on_query_error=False to inspect per-query errors yourself.
report = client.generate_report(
    ["chip-abc"], ["SELECT bogus FROM users"], raise_on_query_error=False,
)
for idx, entry in report.results.items():
    if entry.error:
        print(f"Query {idx} failed: {entry.error}")

# wait_for_ingest raises DatalatheIngestError when an async ingest job ends
# failed or cancelled, and DatalatheIngestTimeoutError when it does not finish
# in time. Both carry the last-observed job record on e.job.
try:
    job = client.wait_for_ingest(job.job_id)
except DatalatheIngestError as e:
    print(f"Ingest failed: {e.job.error}")
except DatalatheIngestTimeoutError as e:
    print(f"Still running after timeout, last status: {e.job.status}")
```

## Requirements

- Python 3.10+
- `requests` >= 2.28

## License

MIT
