def generate_test_file(sql_path, module_key, wrapper_name, test_path):
    code = f"""
import os
import pytest

from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames
from utils.data_loader import (
    discover_table_parquet_info,
    resolve_parquet_for_case,
    load_csv_as_df
)
from utils.csv_schema_resolver import normalize_csv_df
from wrappers.{wrapper_name} import run_{wrapper_name}


SQL_PATH = r"{sql_path}"
MODULE_KEY = r"{module_key}"
MODULE_FOLDER = os.path.join("test_data", MODULE_KEY)


# ---------------------------------------------------------
# Collect input table information
# ---------------------------------------------------------
def _collect_table_info():
    sql_text = load_sql(SQL_PATH)
    tables_full = extract_tables_with_fullnames(sql_text)
    table_names = [table for (_, table) in tables_full]

    info = {{}}
    for table in table_names:
        info[table] = discover_table_parquet_info(MODULE_FOLDER, table)
    return info


# ---------------------------------------------------------
# Build bundle test params (default + all case IDs)
# ---------------------------------------------------------
def _bundle_params():
    info = _collect_table_info()

    # collect all case IDs
    case_ids = set(["default"])
    for t, table_info in info.items():
        case_ids.update(table_info.get("cases", {{}}).keys())

    bundles = []
    for cid in sorted(case_ids):
        mapping = {{}}
        valid = True

        for table, table_info in info.items():

            # case-specific first
            selected = resolve_parquet_for_case(table_info, cid)

            # fallback to default for missing case
            if selected is None:
                selected = table_info.get("default")

            if selected is None:    # no data at all → skip this whole case
                valid = False
                break

            mapping[table] = selected

        if valid:
            bundles.append((cid, mapping))

    return bundles


# ---------------------------------------------------------
# FINAL BUNDLE TEST — Only bundle tests, NO per-table tests
# ---------------------------------------------------------
@pytest.mark.parametrize("caseid,mapping", _bundle_params())
def test_{wrapper_name}_bundle(spark, caseid, mapping):

    print(f"\\n=== Running CASE: {{caseid}} ===")

    # Load ALL required tables for this CASE ID
    for table, csv_path in mapping.items():
        assert os.path.exists(csv_path), (
            f"Missing input file for table '{{table}}': {{csv_path}}"
        )
        df_raw = load_csv_as_df(spark, csv_path)
        df = normalize_csv_df(df_raw, table_name=table)
        df.createOrReplaceTempView(table)

    # Execute SQL
    output_df = run_{wrapper_name}(spark)
    assert output_df is not None

    # Force execution to catch errors
    _ = output_df.count()
""" 
    if os.path.dirname(test_path):
        os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w") as f:
        f.write(code.strip() + "\n")
