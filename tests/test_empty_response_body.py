import pytest
import responses

from datalathe.client import DatalatheClient
from datalathe.errors import DatalatheApiError


@responses.activate
def test_empty_body_on_2xx_raises_api_error():
    responses.add(responses.GET, "http://x/lathe/chips", body="", status=200)
    client = DatalatheClient("http://x")
    with pytest.raises(DatalatheApiError) as ei:
        client.list_chips()
    assert ei.value.status_code == 200


@responses.activate
def test_non_json_body_on_2xx_raises_api_error():
    responses.add(responses.GET, "http://x/lathe/chips", body="<html>oops</html>", status=200)
    client = DatalatheClient("http://x")
    with pytest.raises(DatalatheApiError) as ei:
        client.list_chips()
    assert ei.value.status_code == 200
    assert "oops" in str(ei.value)
