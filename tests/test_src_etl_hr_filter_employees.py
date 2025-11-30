import os
import pytest
from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames
from utils.data_loader import discover_table_parquet_info, resolve_parquet_for_case, load_csv_as_df
from utils.csv_schema_resolver import normalize_csv_df
from wrappers.src.etl.hr.filter_employees import run_filter_employees

SQL_PATH = r"src/etl/hr/filter_employees.sql"
MODULE_KEY = r"src/etl/hr/filter_employees"
MODULE_FOLDER = os.path.join(r"test_data", MODULE_KEY)

def _collect_table_info():
    tables_full = extract_tables_with_fullnames(load_sql(SQL_PATH))
    tables = [b for (_, b) in tables_full]
    info = {}
    for t in tables:
        info[t] = discover_table_parquet_info(MODULE_FOLDER, t)
    return info

def _all_case_ids(table_info):
    s = set()
    for t,i in table_info.items():
        s.update(i.get("cases", {}).keys())
    return sorted(s)

def _per_table_params():
    info = _collect_table_info()
    params = []
    for t,i in info.items():
        if i.get("default"):
            params.append((t, "default", i["default"]))
        for caseid, path in sorted(i.get("cases", {}).items()):
            params.append((t, caseid, path))
    return params

@pytest.mark.parametrize("table,caseid,path", _per_table_params())
def test_filter_employees_per_table(spark, table, caseid, path):
    assert os.path.exists(path), f"Input CSV missing: {path}"
    df_raw = load_csv_as_df(spark, path)
    df = normalize_csv_df(df_raw, table_name=table)
    df.createOrReplaceTempView(table)
    res = run_filter_employees(spark)
    assert res is not None
    _ = res.count()

def _bundle_params():
    info = _collect_table_info()
    case_ids = _all_case_ids(info)
    bundles = []
    for case in case_ids:
        resolved = {}
        skip = False
        for t,i in info.items():
            p = resolve_parquet_for_case(i, case)
            if not p:
                skip = True
                break
            resolved[t] = p
        if not skip:
            bundles.append((case, resolved))
    return bundles

for caseid, mapping in _bundle_params():
    def _make_test(caseid, mapping):
        def test_fn(spark):
            for t, p in mapping.items():
                df_raw = load_csv_as_df(spark, p)
                df = normalize_csv_df(df_raw, table_name=t)
                df.createOrReplaceTempView(t)
            res = run_filter_employees(spark)
            assert res is not None
            _ = res.count()
        test_fn.__name__ = f"test_filter_employees_bundle_{caseid}"
        return test_fn
    globals()[f"test_filter_employees_bundle_{caseid}"] = _make_test(caseid, mapping)
