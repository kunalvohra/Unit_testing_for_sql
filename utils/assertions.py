from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType, DecimalType
)

# Tolerance for floating-point comparison
FLOAT_TOLERANCE = 1e-6


def _coerce_numeric_types(actual, expected):
    """
    Coerce numeric types between actual and expected DataFrames to avoid
    failing test cases due to INT/LONG/FLOAT/DOUBLE mismatches.
    """

    actual_schema = actual.dtypes
    expected_schema = expected.dtypes
    cast_map = {}

    for (col_a, type_a), (col_e, type_e) in zip(actual_schema, expected_schema):

        # Names must match
        if col_a != col_e:
            raise AssertionError(f"Column name mismatch: {col_a} != {col_e}")

        # Skip if already the same
        if type_a == type_e:
            continue

        # INT ↔ LONG
        if {type_a, type_e} == {"int", "bigint"}:
            cast_map[col_a] = IntegerType() if type_e == "int" else LongType()

        # FLOAT ↔ DOUBLE
        elif {type_a, type_e} == {"float", "double"}:
            cast_map[col_a] = DoubleType()

        # INT/LONG ↔ DOUBLE
        elif (type_a in ("int", "bigint") and type_e == "double") or \
             (type_e in ("int", "bigint") and type_a == "double"):
            cast_map[col_a] = DoubleType()

        # DECIMAL ↔ DOUBLE
        elif ("decimal" in type_a and type_e == "double") or \
             ("decimal" in type_e and type_a == "double"):
            cast_map[col_a] = DoubleType()

        else:
            # Unsupported mismatch
            raise AssertionError(
                f"Unsupported type mismatch for column '{col_a}': {type_a} != {type_e}"
            )

    # Apply casts to actual DataFrame
    for col, target_type in cast_map.items():
        actual = actual.withColumn(col, F.col(col).cast(target_type))

    return actual, expected


def assert_df_equal(actual, expected, msg=None):
    """
    Asserts that two Spark DataFrames are equal in:
    - schema (after type coercion)
    - data values (after normalization)
    """

    # Align numeric types
    actual, expected = _coerce_numeric_types(actual, expected)

    # After coercion, schema must match
    assert actual.schema == expected.schema, (
        f"Schema mismatch after type alignment:\n"
        f"ACTUAL: {actual.schema}\nEXPECTED: {expected.schema}\n{msg}"
    )

    # Normalize ordering
    actual_rows = actual.orderBy(*actual.columns).collect()
    expected_rows = expected.orderBy(*expected.columns).collect()

    # Compare row by row
    for a_row, e_row in zip(actual_rows, expected_rows):
        a_vals = list(a_row)
        e_vals = list(e_row)

        for col, (a, e) in enumerate(zip(a_vals, e_vals)):
            # Null-safe check
            if a is None and e is None:
                continue

            # Float/double with tolerance
            if isinstance(a, float) or isinstance(e, float):
                assert abs(float(a) - float(e)) <= FLOAT_TOLERANCE, (
                    f"Float value mismatch in row {a_row}, column {col}: {a} != {e}"
                )
            else:
                # Exact match for non-floats
                assert a == e, (
                    f"Value mismatch in row {a_row}, column {col}: {a} != {e}"
                )

    return True
