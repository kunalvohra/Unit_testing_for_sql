import os
import pytest
from pyspark.sql import SparkSession
from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames
from utils.data_loader import discover_table_parquet_info, load_csv_as_df
from utils.csv_schema_resolver import normalize_csv_df

SQL_ROOTS = ["sql"]
TEST_DATA_ROOT = "test_data"

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("AutoSQLUnitTests")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    return spark

@pytest.fixture(scope="session", autouse=True)
def prepare_global_views(spark):
    print("Preparing global views from CSV test data...")
    for sql_root in SQL_ROOTS:
        if not os.path.isdir(sql_root):
            continue
        for root, _, files in os.walk(sql_root):
            for f in files:
                if not f.endswith(".sql"):
                    continue
                sql_path = os.path.join(root, f).replace("\\", "/")
                module_key = os.path.relpath(sql_path, sql_root).replace(".sql", "").replace("\\", "/")
                base_folder = os.path.join(TEST_DATA_ROOT, module_key)
                if not os.path.isdir(base_folder):
                    continue
                sql_text = load_sql(sql_path)
                tables = extract_tables_with_fullnames(sql_text)
                for full, base in tables:
                    info = discover_table_parquet_info(base_folder, base)
                    p = info.get("default")
                    if not p:
                        print(f"WARNING: No default CSV for table {base} under {base_folder}")
                        continue
                    df_raw = load_csv_as_df(spark, p)
                    df = normalize_csv_df(df_raw, table_name=base)
                    df.createOrReplaceTempView(base)
                    fq = full
                    fq_quoted = f"`{fq}`"
                    df.createOrReplaceTempView(fq_quoted)
                    print(f"Registered views: {base} and {fq_quoted} from {p}")
    print("Global views ready.")
