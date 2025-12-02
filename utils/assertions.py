from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType, DecimalType
)

FLOAT_TOLERANCE = 1e-6

def _coerce_numeric_types(actual, expected):
    actual_schema = actual.dtypes
    expected_schema = expected.dtypes
    cast_map = {}

    for (col_a, type_a), (col_e, type_e) in zip(actual_schema, expected_schema):

        if col_a != col_e:
            raise AssertionError(f"Column name mismatch: {col_a} != {col_e}")

        if type_a == type_e:
            continue

        if {type_a, type_e} == {"int", "bigint"}:
            cast_map[col_a] = IntegerType() if type_e == "int" else LongType()

        elif {type_a, type_e} == {"float", "double"}:
            cast_map[col_a] = DoubleType()

        elif (type_a in ("int", "bigint") and type_e == "double") or              (type_e in ("int", "bigint") and type_a == "double"):
            cast_map[col_a] = DoubleType()

        elif ("decimal" in type_a and type_e == "double") or              ("decimal" in type_e and type_a == "double"):
            cast_map[col_a] = DoubleType()

        else:
            raise AssertionError(
                f"Unsupported type mismatch for column '{col_a}': {type_a} != {type_e}"
            )

    for col, target_type in cast_map.items():
        actual = actual.withColumn(col, F.col(col).cast(target_type))

    return actual, expected


def assert_df_equal(actual, expected, msg=None):

    actual, expected = _coerce_numeric_types(actual, expected)

    assert actual.schema == expected.schema, (
        f"Schema mismatch after type alignment:\n"
        f"ACTUAL: {actual.schema}\nEXPECTED: {expected.schema}\n{msg}"
    )

    actual_rows = actual.orderBy(*actual.columns).collect()
    expected_rows = expected.orderBy(*expected.columns).collect()

    for a_row, e_row in zip(actual_rows, expected_rows):
        for a, e in zip(a_row, e_row):

            if a is None and e is None:
                continue

            if isinstance(a, float) or isinstance(e, float):
                assert abs(float(a) - float(e)) <= FLOAT_TOLERANCE,                     f"Float mismatch: {a} != {e} ({msg})"
            else:
                assert a == e, f"Value mismatch: {a} != {e} ({msg})"

    return True
