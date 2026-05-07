import re

import pytest

from datalathe.errors import ChipNotFoundError, DatalatheApiError, DatalatheStageError
from datalathe.types import SourceRequest, SourceType

pytestmark = pytest.mark.integration


def test_chip_lifecycle(client, csv_path, unique_chip_id):
    chip_id = unique_chip_id
    table_name = "stage_test"

    # create_chip_from_file generates its own chip_id; use create_chips to pin ours.
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
