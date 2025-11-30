import os
import textwrap

# =====================================================================
# Helper: Write files
# =====================================================================
def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(content).strip() + "\n")
    print(f"✔ Created: {path}")


# =====================================================================
# Main Setup Function
# =====================================================================
def setup_framework():

    print("\n🚀 Setting up Complete SQL Bundle Test Framework...\n")

    # =================================================================
    # utils/
    # =================================================================
    write_file("utils/__init__.py", "# utils package\n")

    write_file(
        "utils/sql_loader.py",
        """
        import os
        
        def load_sql(path):
            if not os.path.exists(path):
                raise FileNotFoundError(f"SQL not found: {path}")
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        """
    )

    write_file(
        "utils/sql_table_parser.py",
        """
        import re

        def extract_tables_with_fullnames(sql_text):
            """
            Extract table names referenced in SQL.
            Supports:
                FROM table
                JOIN schema.table
            """
            pattern = r"(?:from|join)\\s+([a-zA-Z0-9_.]+)"
            matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)

            result = []
            for full in matches:
                base = full.split(".")[-1]
                result.append((full, base))

            return result
        """
    )

    write_file(
        "utils/data_loader.py",
        """
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
                cid = p.split("case")[-1].split(".")[0]
                result["cases"][cid] = p

            return result

        def resolve_parquet_for_case(info, caseid):
            return info["cases"].get(caseid)
        """
    )

    write_file(
        "utils/csv_schema_resolver.py",
        """
        def normalize_csv_df(df, table_name=None):
            # Future: add schema enforcement
            return df
        """
    )

    # =================================================================
    # generator/generate_framework.py
    # =================================================================
    write_file(
        "generator/generate_framework.py",
        """
        import os
        import sys

        # Ensure root folder is importable
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

        from utils.sql_loader import load_sql
        from utils.sql_table_parser import extract_tables_with_fullnames


        # -------------------------------------------------------------
        # Helper: derive module key and wrapper name
        # -------------------------------------------------------------
        def derive_module_key(sql_path):
            module_key = os.path.splitext(sql_path)[0]
            wrapper_name = module_key.replace(os.sep, "_")
            return module_key, wrapper_name


        # -------------------------------------------------------------
        # Generate wrapper
        # -------------------------------------------------------------
        def generate_wrapper(sql_path, wrapper_path, wrapper_name):
            code = f\"\"\"
from utils.sql_loader import load_sql

def run_{wrapper_name}(spark):
    sql = load_sql(r"{sql_path}")
    return spark.sql(sql)
\"\"\"
            os.makedirs(os.path.dirname(wrapper_path), exist_ok=True)
            with open(wrapper_path, "w") as f:
                f.write(code.strip() + "\\n")


        # -------------------------------------------------------------
        # Generate bundle-only test file
        # -------------------------------------------------------------
        def generate_test_file(sql_path, module_key, wrapper_name, test_path):
            code = f\"\"\"
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
    tables = [base for (_, base) in tables_full]

    info = {{}}
    for table in tables:
        info[table] = discover_table_parquet_info(MODULE_FOLDER, table)
    return info


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
            selected = resolve_parquet_for_case(table_info, cid)
            if selected is None:
                selected = table_info.get("default")
            if selected is None:  # missing both case & default
                valid = False
                break
            mapping[table] = selected

        if valid:
            bundles.append((cid, mapping))

    return bundles


@pytest.mark.parametrize("caseid, mapping", _bundle_params())
def test_{wrapper_name}_bundle(spark, caseid, mapping):

    print(f"\\n=== Running Bundle CASE: {{caseid}} ===")

    # Load all tables
    for table, csv_path in mapping.items():
        assert os.path.exists(csv_path), f"Missing CSV for {{table}}: {{csv_path}}"
        df_raw = load_csv_as_df(spark, csv_path)
        df = normalize_csv_df(df_raw, table_name=table)
        df.createOrReplaceTempView(table)

    # Execute SQL
    output_df = run_{wrapper_name}(spark)
    assert output_df is not None

    # Force execution
    _ = output_df.count()
\"\"\"
            os.makedirs(os.path.dirname(test_path), exist_ok=True)
            with open(test_path, "w") as f:
                f.write(code.strip() + "\\n")


        # -------------------------------------------------------------
        # Main scanning + generation
        # -------------------------------------------------------------
        def scan_and_generate(sql_src_folder="sql"):
            print(f"\\n🔍 Scanning SQL folder: {sql_src_folder}\\n")

            for root, dirs, files in os.walk(sql_src_folder):
                for fname in files:
                    if not fname.lower().endswith(".sql"):
                        continue

                    sql_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(sql_path)

                    module_key, wrapper_name = derive_module_key(rel_path)

                    # Create test_data folder
                    td_folder = os.path.join("test_data", module_key)
                    os.makedirs(td_folder, exist_ok=True)

                    wrapper_path = os.path.join("wrappers", f"{wrapper_name}.py")
                    test_path = os.path.join("tests", f"test_{wrapper_name}.py")

                    print(f"📄 Found SQL: {rel_path}")
                    print(f"   → Wrapper: {wrapper_path}")
                    print(f"   → Test:    {test_path}")
                    print(f"   → Inputs:  {td_folder}")

                    generate_wrapper(rel_path, wrapper_path, wrapper_name)
                    generate_test_file(rel_path, module_key, wrapper_name, test_path)

            print("\\n✅ Framework generation complete.\\n")


        if __name__ == "__main__":
            scan_and_generate()
        """
    )

    # =================================================================
    # conftest.py
    # =================================================================
    write_file(
        "conftest.py",
        """
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
        """
    )

    # =================================================================
    # Create top-level folders
    # =================================================================
    for folder in ["sql", "test_data", "wrappers", "tests", "generator", "utils"]:
        os.makedirs(folder, exist_ok=True)

    print("\n🎉 COMPLETE SETUP DONE!")
    print("👉 Put SQL files inside folder: sql/")
    print("👉 Then run:")
    print("\n    python generator/generate_framework.py\n")
    print("This will auto-create:")
    print("- Bundle tests (default + caseX)")
    print("- Wrappers")
    print("- test_data/<module_key>/ folders\n")


# =====================================================================
# EXECUTE SETUP
# =====================================================================
if __name__ == "__main__":
    setup_framework()
