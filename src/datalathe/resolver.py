from __future__ import annotations

import logging
import re

from datalathe.client import DatalatheClient, GenerateReportResult
from datalathe.errors import ChipNotFoundError
from datalathe.types import Partition, S3StorageConfig, SourceRequest, TableDef

logger = logging.getLogger(__name__)

_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9 _.:-]{1,128}$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _validate_value(value: str, label: str) -> str:
    if not _SAFE_VALUE_RE.match(value):
        raise ValueError(
            f"Invalid {label}: {value!r} — "
            "must be 1-128 chars of [A-Za-z0-9 _.:-]"
        )
    return value


def _validate_identifier(value: str, label: str) -> str:
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {label}: {value!r} — "
            "must be a valid SQL identifier [A-Za-z_][A-Za-z0-9_]*"
        )
    return value


class ChipResolver:
    """Resolves (find-or-create) chips for SQL queries against DataLathe.

    Wraps a ``DatalatheClient`` with a registry of table definitions so that
    callers can go from raw SQL to query results without manually managing
    chip lifecycle.

    Every table referenced in a query must have a registered ``TableDef``.
    Unregistered tables cause ``resolve_chips`` to raise ``ValueError``.

    Typical usage::

        resolver = ChipResolver(client, table_defs=[
            TableDef("users", "select * from users", tenant_field="org_id"),
            TableDef("orders", "select * from orders",
                     partitioned=True, partition_field="order_date",
                     tenant_field="org_id"),
        ])

        # Low-level: just resolve chip IDs
        chip_ids = resolver.resolve_chips(
            tables=["users", "orders"],
            partition_values=["2024-01-31"],
            tenant_id="42",
        )

        # High-level: extract tables + resolve chips + run report
        result = resolver.query(
            "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id",
            tenant_id="42",
            partition_values=["2024-01-31"],
        )
    """

    def __init__(
        self,
        client: DatalatheClient,
        table_defs: list[TableDef] | None = None,
        tag_key: str = "tenant",
        storage_config: S3StorageConfig | None = None,
    ) -> None:
        self._client = client
        self._tag_key = tag_key
        self._storage_config = storage_config
        self._table_defs: dict[str, TableDef] = {}
        for td in table_defs or []:
            self._table_defs[td.table_name] = td
        self._global_chip_ids: dict[str, str] = {}

    def add_table(self, table_def: TableDef) -> None:
        self._table_defs[table_def.table_name] = table_def

    def register_prewarmed_chip(self, table_name: str, chip_id: str) -> None:
        """Register a chip that was created externally.

        The chip ID is cached the same way as ``warm_global_chips``
        entries — ``resolve_chips`` will reuse it instead of creating a
        new chip for *table_name*.
        """
        self._global_chip_ids[table_name] = chip_id

    def warm_global_chips(self) -> list[str]:
        """Pre-create chips for all tables without a tenant_field.

        These chips contain identical data regardless of tenant, so they only
        need to be created once. Their IDs are cached and reused in all
        subsequent ``resolve_chips`` / ``query`` calls, skipping redundant
        chip creation.

        Returns:
            List of chip IDs created (or already cached).
        """
        created: list[str] = []
        for td in self._table_defs.values():
            if td.tenant_field or td.table_name in self._global_chip_ids:
                continue
            logger.info("Warming global chip for table '%s'", td.table_name)
            ids = self._client.create_chips(
                sources=[SourceRequest(
                    database_name=td.source_name,
                    table_name=td.table_name,
                    query=td.sql,
                )],
                source_type=td.source_type,
                tags={self._tag_key: "global"},
                storage_config=self._storage_config,
            )
            self._global_chip_ids[td.table_name] = ids[0]
            created.append(ids[0])
        logger.info("Warmed %d global chips", len(created))
        return created

    def resolve_chips(
        self,
        tables: list[str],
        partition_values: list[str],
        tenant_id: str,
        force_recreate: bool = False,
    ) -> list[str]:
        """Find existing chips or create new ones for every table needed.

        Args:
            tables: Table names referenced by the query.  Every table must
                have a registered ``TableDef``.
            partition_values: Partition values (e.g. data dates) required for
                partitioned tables.
            tenant_id: Tenant identifier used for tag-based chip isolation.
            force_recreate: When ``True``, skip the cache search and create
                fresh chips for all tables.  Useful after a
                ``ChipNotFoundError`` indicates cached chips have expired.

        Returns:
            List of chip IDs (existing + newly created) suitable for passing
            to ``DatalatheClient.generate_report()``.

        Raises:
            ValueError: If *tables* is empty, contains an unregistered table
                name, or if *tenant_id* / partition values fail validation.
        """
        if not tables:
            raise ValueError("tables must not be empty")
        _validate_value(tenant_id, "tenant_id")
        for pv in partition_values:
            _validate_value(pv, "partition_value")

        global_ids: list[str] = []
        partitioned_tables: set[str] = set()
        unpartitioned_tables: set[str] = set()
        for table in tables:
            _validate_identifier(table, "table name")
            td = self._table_defs.get(table)
            if td is None:
                raise ValueError(
                    f"No TableDef registered for table {table!r} — "
                    "register it via the table_defs constructor argument "
                    "or add_table()"
                )
            if table in self._global_chip_ids:
                global_ids.append(self._global_chip_ids[table])
                continue
            if td.partitioned:
                partitioned_tables.add(table)
            else:
                unpartitioned_tables.add(table)

        seen_unpartitioned: set[str] = set()
        seen_partitioned: set[str] = set()
        existing_unpartitioned_ids: list[str] = []
        existing_partitioned_ids: list[str] = []

        if not force_recreate and (unpartitioned_tables or partitioned_tables):
            tag = f"{self._tag_key}:{tenant_id}"
            existing = self._client.search_chips(tag=tag)
            pv_set = set(partition_values)

            for chip in existing.chips:
                tbl = chip.table_name
                if (
                    tbl in unpartitioned_tables
                    and chip.chip_id == chip.sub_chip_id
                    and tbl not in seen_unpartitioned
                ):
                    seen_unpartitioned.add(tbl)
                    existing_unpartitioned_ids.append(chip.chip_id)
                elif (
                    tbl in partitioned_tables
                    and chip.partition_value in pv_set
                ):
                    key = f"{tbl}|{chip.partition_value}"
                    if key not in seen_partitioned:
                        seen_partitioned.add(key)
                        existing_partitioned_ids.append(chip.chip_id)

        created_ids: list[str] = []
        tags = {self._tag_key: tenant_id}

        for table in sorted(unpartitioned_tables):
            if table in seen_unpartitioned:
                continue
            td = self._table_defs[table]
            sql = td.sql
            if td.tenant_field:
                sql += f" WHERE {td.tenant_field} = '{tenant_id}'"

            logger.info("Creating chip for unpartitioned table '%s'", table)
            ids = self._client.create_chips(
                sources=[SourceRequest(
                    database_name=td.source_name,
                    table_name=table,
                    query=sql,
                )],
                source_type=td.source_type,
                tags=tags,
                storage_config=self._storage_config,
            )
            created_ids.extend(ids)

        for pv in partition_values:
            for table in sorted(partitioned_tables):
                key = f"{table}|{pv}"
                if key in seen_partitioned:
                    continue
                td = self._table_defs[table]
                sql = td.sql
                clauses: list[str] = []
                if td.tenant_field:
                    clauses.append(f"{td.tenant_field} = '{tenant_id}'")
                if td.partition_field:
                    clauses.append(f"{td.partition_field} = '{pv}'")
                if clauses:
                    sql += " WHERE " + " AND ".join(clauses)

                logger.info(
                    "Creating chip for partitioned table '%s' pv='%s'",
                    table, pv,
                )
                ids = self._client.create_chips(
                    sources=[SourceRequest(
                        database_name=td.source_name,
                        table_name=table,
                        query=sql,
                        partition=Partition(
                            partition_by=td.partition_field or "dataDate",
                            partition_values=[pv],
                        ),
                    )],
                    source_type=td.source_type,
                    tags=tags,
                    storage_config=self._storage_config,
                )
                created_ids.extend(ids)

        all_ids = (
            global_ids
            + existing_unpartitioned_ids
            + existing_partitioned_ids
            + created_ids
        )
        logger.info(
            "Resolved %d chips (%d global, %d existing, %d created)",
            len(all_ids),
            len(global_ids),
            len(existing_unpartitioned_ids) + len(existing_partitioned_ids),
            len(created_ids),
        )
        return all_ids

    def query(
        self,
        sql: str,
        tenant_id: str,
        partition_values: list[str] | None = None,
        retry_on_expired: bool = True,
        transform: bool = True,
    ) -> GenerateReportResult:
        """Execute a SQL query through the full DataLathe pipeline.

        Extracts referenced tables from *sql*, resolves (or creates) the
        required chips, and runs the query via ``generate_report``.

        Args:
            sql: The SQL query to execute.
            tenant_id: Tenant identifier for chip isolation.
            partition_values: Partition values for partitioned tables.
            retry_on_expired: If ``True`` (the default), automatically
                re-resolve chips and retry once when a
                ``ChipNotFoundError`` is raised.
            transform: Whether to request MySQL-to-DuckDB SQL transformation.

        Returns:
            The ``GenerateReportResult`` from ``generate_report``.

        Raises:
            ValueError: If ``extract_tables_with_transform`` returns no
                tables, or if chip resolution fails validation.
            ChipNotFoundError: If a chip has expired and *retry_on_expired*
                is ``False``, or if the retry also fails.
            DatalatheApiError: On any other API failure.
        """
        pv = partition_values or []

        extract = self._client.extract_tables_with_transform(
            sql, transform=transform,
        )
        tables: list[str] = extract.get("tables") or []
        if not tables:
            raise ValueError(
                "extract_tables_with_transform returned no tables"
            )
        transformed_query: str = extract.get("transformed_query") or sql

        chip_ids = self.resolve_chips(tables, pv, tenant_id)

        logger.info(
            "Running report with %d chips, query: %s",
            len(chip_ids), transformed_query[:200],
        )
        try:
            result = self._client.generate_report(
                chip_ids, [transformed_query],
            )
        except ChipNotFoundError:
            if not retry_on_expired:
                raise
            logger.warning(
                "Chip expired, re-resolving chips and retrying"
            )
            chip_ids = self.resolve_chips(
                tables, pv, tenant_id, force_recreate=True,
            )
            result = self._client.generate_report(
                chip_ids, [transformed_query],
            )

        return result
