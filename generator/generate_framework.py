import os
import sys
import argparse

# Make root importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames


# ---------------------------------------------------------
# EXCLUDE FOLDERS when scan-all is used
# ---------------------------------------------------------
EXCLUDE_FOLDERS = {
    "generator", "utils", "wrappers", "tests", "test_data",
    ".git", ".idea", ".vscode", "venv", "__pycache__",
}

def is_excluded(path):
    parts = path.replace("\\", "/").split("/")
    return any(p in EXCLUDE_FOLDERS for p in parts)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def derive_module_key(sql_rel_path):
    module_key = os.path.splitext(sql_rel_path)[0]
    module_key = module_key.replace("\\", "/")
    wrapper_name = module_key.replace("/", "_").replace("-", "_")
    return module_key, wrapper_name


def generate_wrapper(sql_rel_path, wrapper_path, wrapper_name):
    code = (
        "from utils.sql_loader import load_sql\n"
        f"def run_{wrapper_name}(spark):\n"
        f"    sql = load_sql(r\"{sql_rel_path}\")\n"
        f"    return spark.sql(sql)\n"
    )
    os.makedirs(os.path.dirname(wrapper_path), exist_ok=True)
    with open(wrapper_path, "w") as f:
        f.write(code)


def generate_test(sql_rel_path, module_key, wrapper_name, test_path):
    code = (
        "import os\n"
        "import pytest\n"
        "from utils.sql_loader import load_sql\n"
        "from utils.sql_table_parser import extract_tables_with_fullnames\n"
        "from utils.data_loader import discover_table_parquet_info, resolve_parquet_for_case, load_csv_as_df\n"
        "from utils.csv_schema_resolver import normalize_csv_df\n"
        f"from wrappers.{wrapper_name} import run_{wrapper_name}\n\n"

        f"SQL_PATH = r\"{sql_rel_path}\"\n"
        f"MODULE_KEY = r\"{module_key}\"\n"
        "MODULE_FOLDER = os.path.join('test_data', MODULE_KEY)\n\n"

        "def _collect_tables():\n"
        "    sql = load_sql(SQL_PATH)\n"
        "    return [b for (_, b) in extract_tables_with_fullnames(sql)]\n\n"

        "def _bundle_params():\n"
        "    tables = _collect_tables()\n"
        "    info = {t: discover_table_parquet_info(MODULE_FOLDER, t) for t in tables}\n"
        "    case_ids = set(['default'])\n"
        "    for t, ti in info.items(): case_ids.update(ti.get('cases', {}).keys())\n"
        "    bundles = []\n"
        "    for cid in sorted(case_ids):\n"
        "        mapping, valid = {}, True\n"
        "        for t, ti in info.items():\n"
        "            p = resolve_parquet_for_case(ti, cid) or ti.get('default')\n"
        "            if not p: valid = False; break\n"
        "            mapping[t] = p\n"
        "        if valid: bundles.append((cid, mapping))\n"
        "    return bundles\n\n"

        "@pytest.mark.parametrize('caseid, mapping', _bundle_params())\n"
        f"def test_{wrapper_name}_bundle(spark, caseid, mapping):\n"
        "    for table, path in mapping.items():\n"
        "        assert os.path.exists(path), f'Missing CSV: {table}: {path}'\n"
        "        df = load_csv_as_df(spark, path)\n"
        "        df = normalize_csv_df(df, table_name=table)\n"
        "        df.createOrReplaceTempView(table)\n"
        "    out = run_{wrapper_name}(spark)\n"
        "    assert out is not None\n"
        "    _ = out.count()\n"
    )
    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w") as f:
        f.write(code)


# ---------------------------------------------------------
# SCAN FUNCTIONS
# ---------------------------------------------------------
def scan_specific_folder(sql_root):
    print(f"\n🔍 Scanning specific folder: {sql_root}\n")

    for base, _, files in os.walk(sql_root):
        for fname in files:
            if not fname.lower().endswith(".sql"):
                continue

            sql_path = os.path.join(base, fname)
            rel_path = os.path.relpath(sql_path).replace("\\", "/")

            _generate_for_sql_file(rel_path)


def scan_entire_repository():
    print("\n🔍 Scanning entire repository (scan-all)...\n")

    for base, dirs, files in os.walk("."):
        if is_excluded(base):
            continue

        for fname in files:
            if not fname.lower().endswith(".sql"):
                continue

            sql_path = os.path.join(base, fname)
            rel_path = os.path.relpath(sql_path).replace("\\", "/")

            _generate_for_sql_file(rel_path)


# ---------------------------------------------------------
# GENERATION FUNCTION
# ---------------------------------------------------------
def _generate_for_sql_file(rel_path):

    module_key, wrapper_name = derive_module_key(rel_path)

    td_folder = os.path.join("test_data", module_key)
    os.makedirs(td_folder, exist_ok=True)

    wrapper_path = os.path.join("wrappers", f"{wrapper_name}.py")
    test_path = os.path.join("tests", f"test_{wrapper_name}.py")

    print(f"📄 SQL: {rel_path}")
    print(f"   → Wrapper: {wrapper_path}")
    print(f"   → Test:    {test_path}")
    print(f"   → Inputs:  {td_folder}\n")

    generate_wrapper(rel_path, wrapper_path, wrapper_name)
    generate_test(rel_path, module_key, wrapper_name, test_path)


# ---------------------------------------------------------
# CLI HANDLER
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL Test Generator")

    parser.add_argument(
        "--sql-root",
        type=str,
        help="Scan only this folder for SQL files (default: sql/)",
        default=None
    )

    parser.add_argument(
        "--scan-all",
        action="store_true",
        help="Scan the entire repo for SQL files"
    )

    args = parser.parse_args()

    if args.scan_all:
        scan_entire_repository()
    else:
        sql_root = args.sql_root or "sql"
        scan_specific_folder(sql_root)

    print("\n✅ Done.\n")
