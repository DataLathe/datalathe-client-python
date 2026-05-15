import os
import re
import time

import pytest

from datalathe.errors import ChipNotFoundError, DatalatheApiError, DatalatheStageError
from datalathe.types import SourceRequest, SourceType

pytestmark = pytest.mark.integration


def _stage_file_chip(client, csv_path, chip_id, table_name):
    client.create_chips(
        sources=[SourceRequest(database_name="", query="", file_path=csv_path, table_name=table_name)],
        source_type=SourceType.FILE,
        chip_id=chip_id,
        chip_name=chip_id,
    )


def _count(client, chip_id, table_name):
    report = client.generate_report(
        chip_ids=[chip_id],
        queries=[f"SELECT COUNT(*) FROM {table_name}"],
    )
    assert 0 in report.results, f"expected key 0, got: {list(report.results.keys())}"
    return str(report.results[0].result[0][0])


def test_chip_lifecycle(client, csv_path, unique_chip_id):
    chip_id = unique_chip_id
    table_name = "stage_test"

    client.create_chips(
        sources=[SourceRequest(database_name="", query="", file_path=csv_path, table_name=table_name)],
        source_type=SourceType.FILE,
        chip_id=chip_id,
        chip_name=f"int-py-{chip_id}",
    )

    chips_resp = client.get_chip(chip_id)
    assert any(c.chip_id == chip_id for c in chips_resp.chips), (
        f"chip {chip_id} not present in get_chip response"
    )

    report = client.generate_report(
        chip_ids=[chip_id],
        queries=[f"SELECT COUNT(*) FROM {table_name}"],
    )
    assert 0 in report.results, f"expected key 0 in report.results, got keys: {list(report.results.keys())}"
    entry = report.results[0]
    cell = entry.result[0][0]
    assert str(cell) == "5", f"expected COUNT(*)=5, got {cell!r}"

    # Engine returns HTTP 409 (TABLE_ALREADY_EXISTS) when re-staging an existing chip.
    # send_command raises DatalatheApiError for non-2xx; DatalatheStageError only fires
    # when the 200-body contains an error field, which won't happen here.
    with pytest.raises((DatalatheStageError, DatalatheApiError)) as exc_info:
        client.create_chips(
            sources=[SourceRequest(database_name="", query="", file_path=csv_path, table_name=table_name)],
            source_type=SourceType.FILE,
            chip_id=chip_id,
            chip_name=f"int-py-{chip_id}",
        )
    assert re.search(r"TABLE_ALREADY_EXISTS|already exists", str(exc_info.value), re.I), (
        f"unexpected error message: {exc_info.value}"
    )

    client.delete_chip(chip_id)

    # get_chip raises ChipNotFoundError (subclass of DatalatheApiError) on 404
    # when the engine returns error_code: chip_not_found.
    with pytest.raises(ChipNotFoundError):
        client.get_chip(chip_id)


def test_create_chip_from_chip_unions_sources(client, csv_path, chip_tracker):
    """Two 5-row source chips sharing a table_name must merge into one 10-row
    chip. Reading the merged chip back confirms it was finalized to disk."""
    table_name = "loan03"
    base = f"int-py-cfc-{int(time.time() * 1000)}-{os.getpid()}"
    chip_a = chip_tracker(f"{base}-a")
    chip_b = chip_tracker(f"{base}-b")
    _stage_file_chip(client, csv_path, chip_a, table_name)
    _stage_file_chip(client, csv_path, chip_b, table_name)

    merged = chip_tracker(
        client.create_chip_from_chip(
            source_chip_ids=[chip_a, chip_b],
            query=f"SELECT * FROM {table_name}",
            table_name=table_name,
            chip_name=f"{base}-merged",
        )
    )

    count = _count(client, merged, table_name)
    assert count == "10", f"expected unioned COUNT(*)=10, got {count!r}"


def test_create_chip_from_chip_without_query_copies_source(client, csv_path, chip_tracker):
    """Omitting the query must copy the source table verbatim. Both SDKs send
    query="" rather than null, so the engine must treat empty as 'no query'."""
    table_name = "loan03"
    base = f"int-py-cfc-noq-{int(time.time() * 1000)}-{os.getpid()}"
    chip_a = chip_tracker(f"{base}-a")
    _stage_file_chip(client, csv_path, chip_a, table_name)

    merged = chip_tracker(
        client.create_chip_from_chip(
            source_chip_ids=[chip_a],
            table_name=table_name,
            chip_name=f"{base}-copy",
        )
    )

    count = _count(client, merged, table_name)
    assert count == "5", f"expected copied COUNT(*)=5, got {count!r}"
