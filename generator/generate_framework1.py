import os
from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames


# ---------------------------------------------------------
# Helper: module key + wrapper name
# ---------------------------------------------------------
def derive_module_key(sql_path):
    module_key = os.path.splitext(sql_path)[0]
    wrapper_name = module_key.replace(os.sep, "_")
    return module_key, wrapper_name


# ---------------------------------------------------------
# Generate Wrapper Python file
# ---------------------------------------------------------
def generate_wrapper(sql_path, wrapper_path, wrapper_name):
    code = f"""
from utils.sql_loader import load_sql

def run_{wrapper_name}(spark):
    sql = load_sql(r"{sql_path}")
    return spark.sql(sql)
"""
    os.makedirs(os.path.dirname(wrapper_path), exist_ok=True)
    with open(wrapper_path, "w", encoding="utf-8") as f:
        f.write(code.strip() + "\n")


# ---------------------------------------------------------
# Generate BUNDLE-ONLY test file
# ---------------------------------------------------------
def generate_test_file(sql_path, module_key, wrapper_name, test_path):
    test_code = f"""
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


def _collect_table_info():
    sql_text = load_sql(SQL_PATH)
    tables_full = extract_tables_with_fullnames(sql_text)
    table_names = [base for (_, base) in tables_full]

    info = {{}}
    for t in table_names:
        info[t] = discover_table_parquet_info(MODULE_FOLDER, t)
    return info


def _bundle_params():
    info = _collect_table_info()

    case_ids = set(["default"])
    for t, ti in info.items():
        case_ids.update(ti.get("cases", {{}}).keys())

    bundles = []
    for cid in sorted(case_ids):
        mapping = {{}}
        valid = True

        for table, ti in info.items():
            selected = resolve_parquet_for_case(ti, cid)
            if selected is None:
                selected = ti.get("default")
            if selected is None:
                valid = False
                break
            mapping[table] = selected

        if valid:
            bundles.append((cid, mapping))

    return bundles


@pytest.mark.parametrize("caseid, mapping", _bundle_params())
def test_{wrapper_name}_bundle(spark, caseid, mapping):

    for table, csv_path in mapping.items():
        assert os.path.exists(csv_path), f"Missing CSV: {{csv_path}}"

        df_raw = load_csv_as_df(spark, csv_path)
        df = normalize_csv_df(df_raw, table_name=table)
        df.createOrReplaceTempView(table)

    result_df = run_{wrapper_name}(spark)
    assert result_df is not None
    _ = result_df.count()
"""
    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w", encoding="utf-8") as f:
        f.write(test_code.strip() + "\n")


# ---------------------------------------------------------
# MASTER GENERATOR (renamed parameter)
# ---------------------------------------------------------
def scan_and_generate(sql_src_folder="sql"):
    print(f"\n🔍 Scanning for SQL files inside: {sql_src_folder}\n")

    for root, dirs, files in os.walk(sql_src_folder):
        for fname in files:
            if not fname.lower().endswith(".sql"):
                continue

            sql_path = os.path.join(root, fname)
            rel_path = os.path.relpath(sql_path)

            module_key, wrapper_name = derive_module_key(rel_path)

            wrapper_path = os.path.join("wrappers", f"{wrapper_name}.py")
            test_path = os.path.join("tests", f"test_{wrapper_name}.py")

            print(f"📄 Found SQL: {rel_path}")
            print(f"   → Generating wrapper: {wrapper_path}")
            print(f"   → Generating test:    {test_path}")

            generate_wrapper(rel_path, wrapper_path, wrapper_name)
            generate_test_file(rel_path, module_key, wrapper_name, test_path)

    print("\n✅ Framework generation complete.\n")


if __name__ == "__main__":
    scan_and_generate()
