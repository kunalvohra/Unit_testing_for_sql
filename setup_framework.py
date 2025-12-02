import os
import textwrap

def write_file(path, content):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(content).lstrip())

    print(f"✔ Created: {path}")


def setup_framework():

    print("\n🚀 Creating FULL SQL Testing Framework...\n")

    # ============================================================
    # utils/__init__.py
    # ============================================================
    write_file("utils/__init__.py", "# utils package\n")

    # ============================================================
    # utils/sql_loader.py
    # ============================================================
    write_file("utils/sql_loader.py", """
import os

def load_sql(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
""")

    # ============================================================
    # utils/sql_table_parser.py
    # ============================================================
    write_file("utils/sql_table_parser.py", r"""
import re

def extract_tables_with_fullnames(sql_text):
    pattern = r"(?:from|join)\s+([a-zA-Z0-9_.]+)"
    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)
    tables = []
    for full in matches:
        base = full.split(".")[-1]
        tables.append((full, base))
    return tables
""")

    # ============================================================
    # utils/data_loader.py
    # ============================================================
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
        cid = os.path.basename(p).split("case")[-1].split(".")[0]
        result["cases"][cid] = p

    return result

def resolve_parquet_for_case(info, caseid):
    return info["cases"].get(caseid)
""")

    # ============================================================
    # utils/csv_schema_resolver.py
    # ============================================================
    write_file("utils/csv_schema_resolver.py", """
def normalize_csv_df(df, table_name=None):
    return df
""")

    # ============================================================
    # utils/assertions.py (UPDATED — FLOAT/DOUBLE/LONG/INT)
    # ============================================================
    write_file("utils/assertions.py", """
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

        elif (type_a in ("int", "bigint") and type_e == "double") or \
             (type_e in ("int", "bigint") and type_a == "double"):
            cast_map[col_a] = DoubleType()

        elif ("decimal" in type_a and type_e == "double") or \
             ("decimal" in type_e and type_a == "double"):
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
        f"Schema mismatch after type alignment:\\n"
        f"ACTUAL: {actual.schema}\\nEXPECTED: {expected.schema}\\n{msg}"
    )

    actual_rows = actual.orderBy(*actual.columns).collect()
    expected_rows = expected.orderBy(*expected.columns).collect()

    for a_row, e_row in zip(actual_rows, expected_rows):
        for a, e in zip(a_row, e_row):

            if a is None and e is None:
                continue

            if isinstance(a, float) or isinstance(e, float):
                assert abs(float(a) - float(e)) <= FLOAT_TOLERANCE, \
                    f"Float mismatch: {a} != {e} ({msg})"
            else:
                assert a == e, f"Value mismatch: {a} != {e} ({msg})"

    return True
""")

    # ============================================================
    # conftest.py
    # ============================================================
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

    # ============================================================
    # generator/generate_framework.py
    # ============================================================
    write_file("generator/generate_framework.py", r'''
import os
import sys
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames


EXCLUDE = {"generator","utils","wrappers","tests","test_data",".git",".vscode","venv","__pycache__"}

def is_excluded(path):
    parts = path.replace("\\", "/").split("/")
    return any(p in EXCLUDE for p in parts)

def derive_module_key(rel_path):
    module_key = os.path.splitext(rel_path)[0]
    module_key = module_key.replace("\\", "/")
    wrapper_name = module_key.replace("/", "_")
    return module_key, wrapper_name


def generate_wrapper(rel_path, wrapper_path, wrapper_name):
    code = (
        "from utils.sql_loader import load_sql\n"
        f"def run_{wrapper_name}(spark):\n"
        f"    sql = load_sql(r\"{rel_path}\")\n"
        f"    return spark.sql(sql)\n"
    )
    os.makedirs(os.path.dirname(wrapper_path), exist_ok=True)
    with open(wrapper_path, "w") as f:
        f.write(code)


def generate_test(rel_path, module_key, wrapper_name, test_path):
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
        "            if not p: valid = False; break\n"
        "            mapping[t] = p\n"
        "        if valid: bundles.append((cid, mapping))\n"
        "    return bundles\n\n"

        "@pytest.mark.parametrize('caseid, mapping', _bundle_params())\n"
        f"def test_{wrapper_name}_bundle(spark, caseid, mapping):\n"
        "    for table, path in mapping.items():\n"
        "        df = load_csv_as_df(spark, path)\n"
        "        df = normalize_csv_df(df, table_name=table)\n"
        "        df.createOrReplaceTempView(table)\n\n"

        f"    actual = run_{wrapper_name}(spark)\n\n"

        "    expected_path = os.path.join(MODULE_FOLDER, f\"expected_{caseid}.csv\")\n"
        "    assert os.path.exists(expected_path), f\"Missing expected output: {expected_path}\"\n\n"

        "    expected = load_csv_as_df(spark, expected_path)\n"
        "    expected = normalize_csv_df(expected)\n\n"

        f"    assert_df_equal(actual, expected, msg=f'{wrapper_name} - {{caseid}}')\n"
    )

    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w") as f:
        f.write(code)


def process_sql(rel_path):
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


def scan_folder(root):
    for base, _, files in os.walk(root):
        for f in files:
            if f.endswith(".sql"):
                rel = os.path.relpath(os.path.join(base, f)).replace("\\", "/")
                process_sql(rel)


def scan_all():
    for base, _, files in os.walk("."):
        if is_excluded(base): continue
        for f in files:
            if f.endswith(".sql"):
                rel = os.path.relpath(os.path.join(base, f)).replace("\\", "/")
                process_sql(rel)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-root", type=str)
    parser.add_argument("--scan-all", action="store_true")
    args = parser.parse_args()

    if args.scan_all:
        scan_all()
    else:
        root = args.sql_root or "sql"
        scan_folder(root)

    print("\n✔ DONE.\n")

'''
    )

    # ============================================================
    # Create empty folder structure
    # ============================================================
    for folder in ["utils", "tests", "wrappers", "generator", "test_data", "sql"]:
        os.makedirs(folder, exist_ok=True)

    print("\n🎉 FRAMEWORK SETUP COMPLETE!")
    print("Next Steps:")
    print("1️⃣ Put SQL files under ./sql/ or anywhere else")
    print("2️⃣ Generate tests:")
    print("   python generator/generate_framework.py --scan-all")
    print("3️⃣ Add input CSVs:")
    print("   test_data/<module_key>/<table>.csv")
    print("4️⃣ Add expected outputs:")
    print("   test_data/<module_key>/expected_<case>.csv")
    print("5️⃣ Run tests:")
    print("   pytest -q\n")


if __name__ == "__main__":
    setup_framework()
