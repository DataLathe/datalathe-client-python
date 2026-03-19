from datalathe import DatalatheResultSet, ReportResultEntry, SchemaField


def _make_result() -> ReportResultEntry:
    return ReportResultEntry(
        idx="0",
        result=[
            ["Alice", "30", "1.75", "true"],
            ["Bob", None, "1.80", "false"],
            ["Charlie", "25", "1.68", None],
        ],
        schema=[
            SchemaField(name="name", data_type="Utf8"),
            SchemaField(name="age", data_type="Int32"),
            SchemaField(name="height", data_type="Float64"),
            SchemaField(name="active", data_type="Boolean"),
        ],
    )


def test_next_and_get_string():
    rs = DatalatheResultSet(_make_result())
    assert rs.next()
    assert rs.get_string(1) == "Alice"
    assert rs.next()
    assert rs.get_string(1) == "Bob"


def test_get_int():
    rs = DatalatheResultSet(_make_result())
    rs.next()
    assert rs.get_int(2) == 30
    rs.next()
    assert rs.get_int(2) == 0
    assert rs.was_null()


def test_get_float():
    rs = DatalatheResultSet(_make_result())
    rs.next()
    assert rs.get_float(3) == 1.75


def test_get_boolean():
    rs = DatalatheResultSet(_make_result())
    rs.next()
    assert rs.get_boolean(4) is True
    rs.next()
    assert rs.get_boolean(4) is False


def test_get_object_type_conversion():
    rs = DatalatheResultSet(_make_result())
    rs.next()
    assert rs.get_object(1) == "Alice"
    assert rs.get_object(2) == 30
    assert rs.get_object(3) == 1.75
    assert rs.get_object(4) is True


def test_find_column():
    rs = DatalatheResultSet(_make_result())
    assert rs.find_column("age") == 2
    assert rs.find_column("AGE") == 2


def test_column_by_name():
    rs = DatalatheResultSet(_make_result())
    rs.next()
    assert rs.get_string("name") == "Alice"
    assert rs.get_int("age") == 30


def test_navigation():
    rs = DatalatheResultSet(_make_result())
    assert rs.is_before_first()
    assert rs.first()
    assert rs.is_first()
    assert rs.last()
    assert rs.is_last()
    assert rs.get_string(1) == "Charlie"
    assert rs.previous()
    assert rs.get_string(1) == "Bob"


def test_absolute():
    rs = DatalatheResultSet(_make_result())
    assert rs.absolute(2)
    assert rs.get_string(1) == "Bob"
    assert rs.absolute(-1)
    assert rs.get_string(1) == "Charlie"


def test_to_list():
    rs = DatalatheResultSet(_make_result())
    rows = rs.to_list()
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 30
    assert rows[1]["age"] is None


def test_iter():
    rs = DatalatheResultSet(_make_result())
    names = [row["name"] for row in rs]
    assert names == ["Alice", "Bob", "Charlie"]


def test_len():
    rs = DatalatheResultSet(_make_result())
    assert len(rs) == 3
    assert rs.row_count == 3


def test_metadata():
    rs = DatalatheResultSet(_make_result())
    assert rs.get_column_count() == 4
    assert rs.get_column_name(1) == "name"
    assert rs.get_column_type(2) == "Int32"
    schema = rs.get_schema()
    assert len(schema) == 4
