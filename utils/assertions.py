def assert_df_equal(actual, expected, msg=None):
    assert actual.schema == expected.schema, f"Schema mismatch: {msg}"

    actual_rows = [tuple(r) for r in actual.orderBy(*actual.columns).collect()]
    expected_rows = [tuple(r) for r in expected.orderBy(*expected.columns).collect()]

    assert actual_rows == expected_rows, f"Data mismatch: {msg}"
