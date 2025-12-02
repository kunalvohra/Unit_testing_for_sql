import os
import sys
import argparse

# Make project root importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames

# Folders to ignore when scanning whole repo
EXCLUDE = {".ipynb_checkpoints","generator", "utils", "wrappers", "tests", "test_data", ".git", ".vscode", "venv", "__pycache__"}

def is_excluded(path):
    parts = path.replace("\\", "/").split("/")
    return any(p in EXCLUDE for p in parts)

def derive_module_key(rel_path):
    module_key = os.path.splitext(rel_path)[0]
    module_key = module_key.replace("\\", "/")
    wrapper_name = module_key.replace("/", "_").replace("-", "_").replace(" ", "_")
    return module_key, wrapper_name

def generate_wrapper(rel_path, wrapper_path, wrapper_name):
    code = (
        "from utils.sql_loader import load_sql\n"
        f"def run_{wrapper_name}(spark):\n"
        f"    sql = load_sql(r\"{rel_path}\")\n"
        f"    return spark.sql(sql)\n"
    )
    folder = os.path.dirname(wrapper_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    with open(wrapper_path, "w", encoding="utf-8") as f:
        f.write(code)

def generate_test(rel_path, module_key, wrapper_name, test_path):
    # Note: wrapper_name is injected now; caseid stays dynamic via {{caseid}}
    code = (
        "import os\n"
        "import pytest\n"
        "from utils.sql_loader import load_sql\n"
        "from utils.sql_table_parser import extract_tables_with_fullnames\n"
        "from utils.data_loader import discover_table_parquet_info, resolve_parquet_for_case, load_csv_as_df\n"
        "from utils.csv_schema_resolver import normalize_csv_df\n"
        "from utils.assertions import assert_df_equal\n"
        f"from wrappers.{wrapper_name} import run_{wrapper_name}\n\n"

        f"SQL_PATH = r\"{rel_path}\"\n"
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
        "            if not p:\n"
        "                valid = False\n"
        "                break\n"
        "            mapping[t] = p\n"
        "        if valid:\n"
        "            bundles.append((cid, mapping))\n"
        "    return bundles\n\n"

        "@pytest.mark.parametrize('caseid, mapping', _bundle_params())\n"
        f"def test_{wrapper_name}_bundle(spark, caseid, mapping):\n"
        "    # Load all input tables for this bundle\n"
        "    for table, path in mapping.items():\n"
        "        assert os.path.exists(path), f'Missing input CSV for {table}: {path}'\n"
        "        df = load_csv_as_df(spark, path)\n"
        "        df = normalize_csv_df(df, table_name=table)\n"
        "        df.createOrReplaceTempView(table)\n\n"

        "    # Execute SQL via wrapper\n"
        f"    actual = run_{wrapper_name}(spark)\n\n"

        "    # Load expected output for this case\n"
        "    expected_path = os.path.join(MODULE_FOLDER, f\"expected_{{caseid}}.csv\")\n"
        "    assert os.path.exists(expected_path), f'Missing expected output: {expected_path}'\n"
        "    expected = load_csv_as_df(spark, expected_path)\n"
        "    expected = normalize_csv_df(expected)\n\n"

        # wrapper_name is generated; caseid is runtime variable — keep braces escaped
        f"    assert_df_equal(actual, expected, msg=f'{wrapper_name} - {{caseid}}')\n"
    )

    folder = os.path.dirname(test_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    with open(test_path, "w", encoding="utf-8") as f:
        f.write(code)

def _create_blank_inputs_for_tables(module_key, tables):
    folder = os.path.join("test_data", module_key)
    os.makedirs(folder, exist_ok=True)
    for t in tables:
        csv_path = os.path.join(folder, f"{t}.csv")
        if not os.path.exists(csv_path):
            # create an empty CSV file (zero bytes) — user will populate headers/rows
            with open(csv_path, "w", encoding="utf-8") as fh:
                fh.write("")
            print(f"   → Created blank CSV: {csv_path}")
        else:
            print(f"   → Input exists: {csv_path}")

def process_sql(rel_path):
    module_key, wrapper_name = derive_module_key(rel_path)

    # Ensure test_data folder exists for this SQL module
    td = os.path.join("test_data", module_key)
    os.makedirs(td, exist_ok=True)

    # Parse SQL and auto-create blank CSVs for referenced tables
    try:
        sql_text = load_sql(rel_path)
        tables_full = extract_tables_with_fullnames(sql_text)
        tables = [base for (_, base) in tables_full]
    except Exception as e:
        print(f"Warning: failed to parse tables from {rel_path}: {e}")
        tables = []

    if tables:
        _create_blank_inputs_for_tables(module_key, tables)

    wrapper_path = os.path.join("wrappers", f"{wrapper_name}.py")
    test_path = os.path.join("tests", f"test_{wrapper_name}.py")

    print(f"📄 SQL: {rel_path}")
    print(f"   → Wrapper: {wrapper_path}")
    print(f"   → Test:    {test_path}")
    print(f"   → Inputs:  {td}\n")

    generate_wrapper(rel_path, wrapper_path, wrapper_name)
    generate_test(rel_path, module_key, wrapper_name, test_path)

def scan_folder(root):
    if not os.path.exists(root):
        print(f"Folder not found: {root}")
        return
    print(f"Scanning folder: {root}")
    for base, _, files in os.walk(root):
        for fname in files:
            if fname.lower().endswith(".sql"):
                rel = os.path.relpath(os.path.join(base, fname)).replace("\\", "/")
                process_sql(rel)

def scan_all():
    print("Scanning entire repository (excluding framework folders)...")
    for base, _, files in os.walk("."):
        if is_excluded(base):
            continue
        for fname in files:
            if fname.lower().endswith(".sql"):
                rel = os.path.relpath(os.path.join(base, fname)).replace("\\", "/")
                process_sql(rel)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL test wrappers and tests")
    parser.add_argument("--sql-root", type=str, help="Scan only this folder for .sql files", default=None)
    parser.add_argument("--scan-all", action="store_true", help="Scan the entire repository for .sql files")
    args = parser.parse_args()

    if args.scan_all:
        scan_all()
    else:
        root = args.sql_root or "sql"
        scan_folder(root)

    print("\\nGeneration finished.\\n")
