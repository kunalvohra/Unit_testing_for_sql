import os
import textwrap


# ---------------------------------------------------------
# Safe file writer (Windows proof)
# ---------------------------------------------------------
def write_file(path, content):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(content).lstrip())

    print(f"✔ Created: {path}")


# ---------------------------------------------------------
# MAIN SETUP
# ---------------------------------------------------------
def setup_framework():

    print("\n🚀 Setting up SQL Bundle Unit Test Framework...\n")

    # =====================================================
    # UTILS
    # =====================================================

    write_file("utils/__init__.py", "# utils package\n")

    write_file("utils/sql_loader.py", """
import os

def load_sql(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
""")

    write_file("utils/sql_table_parser.py", r"""
import re

def extract_tables_with_fullnames(sql_text):
    # Extract tables referenced by FROM or JOIN
    pattern = r"(?:from|join)\s+([a-zA-Z0-9_.]+)"
    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)

    output = []
    for full in matches:
        base = full.split(".")[-1]
        output.append((full, base))
    return output
""")

    write_file("utils/data_loader.py", """
import os
import glob

def load_csv_as_df(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def discover_table_parquet_info(folder, table):
    result = {"default": None, "cases": {}}

    default_path = os.path.join(folder, f"{table}.csv")
    if os.path.exists(default_path):
        result["default"] = default_path

    for p in glob.glob(os.path.join(folder, f"{table}_case*.csv")):
        file = os.path.basename(p)
        cid = file.split("case")[-1].split(".")[0]
        result["cases"][cid] = p

    return result

def resolve_parquet_for_case(info, caseid):
    return info["cases"].get(caseid)
""")

    write_file("utils/csv_schema_resolver.py", """
def normalize_csv_df(df, table_name=None):
    return df
""")

    # =====================================================
    # conftest.py
    # =====================================================
    write_file("conftest.py", """
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SQLBundleTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
""")

    # =====================================================
    # generator/generate_framework.py
    # =====================================================
    generator_code = r'''
import os
import sys

# Add project root to import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames


def derive_module_key(sql_rel_path):
    module_key = os.path.splitext(sql_rel_path)[0]
    wrapper_name = module_key.replace(os.sep, "_").replace("-", "_")
    return module_key, wrapper_name


def generate_wrapper(sql_rel_path, wrapper_path, wrapper_name):
    code = (
        "from utils.sql_loader import load_sql\n"
        f"def run_{wrapper_name}(spark):\n"
        f"    sql = load_sql(r\"{sql_rel_path}\")\n"
        f"    return spark.sql(sql)\n"
    )
    folder = os.path.dirname(wrapper_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
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
        "    refs = extract_tables_with_fullnames(sql)\n"
        "    return [b for (_, b) in refs]\n\n"

        "def _bundle_params():\n"
        "    tables = _collect_tables()\n"
        "    info = {t: discover_table_parquet_info(MODULE_FOLDER, t) for t in tables}\n"
        "    case_ids = set(['default'])\n"
        "    for t, ti in info.items():\n"
        "        case_ids.update(ti.get('cases', {}).keys())\n"
        "    bundles = []\n"
        "    for cid in sorted(case_ids):\n"
        "        mapping = {}\n"
        "        valid = True\n"
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
        "    for table, path in mapping.items():\n"
        "        assert os.path.exists(path), f'Missing file: {table}: {path}'\n"
        "        df = load_csv_as_df(spark, path)\n"
        "        df = normalize_csv_df(df, table_name=table)\n"
        "        df.createOrReplaceTempView(table)\n"
        f"    out = run_{wrapper_name}(spark)\n"
        "    assert out is not None\n"
        "    _ = out.count()\n"
    )
    folder = os.path.dirname(test_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    with open(test_path, "w") as f:
        f.write(code)


def scan_and_generate(sql_folder='sql'):
    print('\\nScanning folder:', sql_folder)
    for root, dirs, files in os.walk(sql_folder):
        for fname in files:
            if not fname.lower().endswith('.sql'):
                continue
            sql_path = os.path.join(root, fname)
            rel = os.path.relpath(sql_path)
            module_key, wrapper_name = derive_module_key(rel)
            td = os.path.join('test_data', module_key)
            os.makedirs(td, exist_ok=True)
            wrapper_path = os.path.join('wrappers', f'{wrapper_name}.py')
            test_path = os.path.join('tests', f'test_{wrapper_name}.py')

            print('Found SQL:', rel)
            generate_wrapper(rel, wrapper_path, wrapper_name)
            generate_test(rel, module_key, wrapper_name, test_path)

    print('\\n✔ Generation complete!')

if __name__ == '__main__':
    scan_and_generate()
'''

    write_file("generator/generate_framework.py", generator_code)

    # =====================================================
    # Create top folders
    # =====================================================
    for d in ["sql", "test_data", "utils", "generator", "wrappers", "tests"]:
        os.makedirs(d, exist_ok=True)

    print("\n🎉 SETUP COMPLETE!")
    print("Next steps:")
    print("1️⃣ Put SQL files inside: sql/")
    print("2️⃣ Generate tests: python generator/generate_framework.py")
    print("3️⃣ Add CSVs inside test_data/<module>/")
    print("4️⃣ Run: pytest -q\n")


# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    setup_framework()
