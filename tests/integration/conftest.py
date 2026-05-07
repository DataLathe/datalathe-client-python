import os
import time

import pytest

from datalathe import DatalatheClient


@pytest.fixture(scope="session")
def datalathe_url() -> str:
    url = os.environ.get("DATALATHE_URL")
    if not url:
        pytest.skip("DATALATHE_URL not set; integration tests require a running engine")
    return url


@pytest.fixture(scope="session")
def csv_path() -> str:
    return os.environ.get("E2E_CSV_PATH", "/tmp/test-data.csv")


@pytest.fixture
def client(datalathe_url: str) -> DatalatheClient:
    return DatalatheClient(datalathe_url)


@pytest.fixture
def unique_chip_id() -> str:
    return f"int-py-{int(time.time() * 1000)}-{os.getpid()}"
