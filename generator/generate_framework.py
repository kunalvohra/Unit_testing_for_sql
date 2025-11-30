#!/usr/bin/env python3
"""Simple generator helper included in the bundle.
This script discovers SQL files and writes simple wrapper and test files.
It is intentionally minimal; it is a scaffolding helper.
"""
import os
import re
import argparse
from pathlib import Path
from textwrap import dedent
import yaml

SQL_ROOT_DEFAULT = "sql"
WRAPPER_ROOT = "wrappers"
TEST_ROOT = "tests"
TEST_DATA_ROOT = "test_data"
TEST_CFG = "test_config/test_map.yaml"

WRAPPER_TEMPLATE = '''from utils.sql_loader import load_sql

def {function_name}(spark):
    """Auto-generated wrapper to execute SQL: {sql_path}"""
    sql = load_sql(r"{sql_path}")
    return spark.sql(sql)
'''

TEST_TEMPLATE = dedent('''import os
import pytest
from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames
from utils.data_loader import discover_table_parquet_info, resolve_parquet_for_case, load_csv_as_df
from utils.csv_schema_resolver import normalize_csv_df
from {import_path} import {function_name}

SQL_PATH = r"{sql_path}"
MODULE_KEY = r"{module_key}"
MODULE_FOLDER = os.path.join(r"{test_data_root}", MODULE_KEY)

def _collect_table_info():
    tables_full = extract_tables_with_fullnames(load_sql(SQL_PATH))
    tables = [b for (_, b) in tables_full]
    info = {{}}
    for t in tables:
        info[t] = discover_table_parquet_info(MODULE_FOLDER, t)
    return info

def _all_case_ids(table_info):
    s = set()
    for t,i in table_info.items():
        s.update(i.get("cases", {{}}).keys())
    return sorted(s)

def _per_table_params():
    info = _collect_table_info()
    params = []
    for t,i in info.items():
        if i.get("default"):
            params.append((t, "default", i["default"]))
        for caseid, path in sorted(i.get("cases", {{}}).items()):
            params.append((t, caseid, path))
    return params

@pytest.mark.parametrize("table,caseid,path", _per_table_params())
def test_{base_name}_per_table(spark, table, caseid, path):
    assert os.path.exists(path), f"Input CSV missing: {{path}}"
    df_raw = load_csv_as_df(spark, path)
    df = normalize_csv_df(df_raw, table_name=table)
    df.createOrReplaceTempView(table)
    res = {function_name}(spark)
    assert res is not None
    _ = res.count()

def _bundle_params():
    info = _collect_table_info()
    case_ids = _all_case_ids(info)
    bundles = []
    for case in case_ids:
        resolved = {{}}
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
            res = {function_name}(spark)
            assert res is not None
            _ = res.count()
        test_fn.__name__ = f"test_{base_name}_bundle_{{caseid}}"
        return test_fn
    globals()[f"test_{base_name}_bundle_{{caseid}}"] = _make_test(caseid, mapping)
''')

def ensure_dirs():
    Path(WRAPPER_ROOT).mkdir(parents=True, exist_ok=True)
    Path(TEST_ROOT).mkdir(parents=True, exist_ok=True)
    Path(TEST_DATA_ROOT).mkdir(parents=True, exist_ok=True)
    Path("test_config").mkdir(parents=True, exist_ok=True)
    Path("utils").mkdir(parents=True, exist_ok=True)

def snake(s: str) -> str:
    return re.sub(r'[^0-9a-zA-Z_]+', '_', s).strip('_').lower()

def load_or_create_cfg():
    if os.path.exists(TEST_CFG):
        with open(TEST_CFG, 'r') as f:
            import yaml
            return yaml.safe_load(f) or {}
    return {}

def save_cfg(cfg):
    with open(TEST_CFG, 'w', encoding='utf-8') as f:
        import yaml
        yaml.dump(cfg, f, sort_keys=True)

def discover_sql_files(sql_root, scan_all=False):
    files = []
    if scan_all:
        for p in Path('.').rglob("*.sql"):
            files.append(str(p).replace("\\", "/"))
    else:
        for p in Path(sql_root).rglob("*.sql"):
            files.append(str(p).replace("\\", "/"))
    return sorted(files)

def import_path_from_wrapper(wrapper_path):
    p = Path(wrapper_path)
    parts = list(p.with_suffix('').parts)
    return ".".join(parts)

def generate(sql_root, scan_all):
    ensure_dirs()
    cfg = load_or_create_cfg()
    files = discover_sql_files(sql_root, scan_all)
    if not files:
        print("No SQL files found.")
        return
    for sql_path in files:
        if not scan_all and sql_path.startswith(sql_root):
            rel = os.path.relpath(sql_path, sql_root)
        else:
            rel = os.path.relpath(sql_path)
        module_key = rel.replace("\\", "/").replace(".sql", "")
        wrapper_path = os.path.join(WRAPPER_ROOT, rel.replace(".sql", ".py"))
        test_path = os.path.join(TEST_ROOT, f"test_{snake(module_key.replace('/', '_'))}.py")
        os.makedirs(os.path.dirname(wrapper_path), exist_ok=True)
        os.makedirs(os.path.dirname(test_path), exist_ok=True)
        function_name = f"run_{snake(Path(sql_path).stem)}"
        import_path = import_path_from_wrapper(wrapper_path)
        with open(wrapper_path, 'w', encoding='utf-8') as wf:
            wf.write(WRAPPER_TEMPLATE.format(function_name=function_name, sql_path=sql_path))
        with open(test_path, 'w', encoding='utf-8') as tf:
            tf.write(TEST_TEMPLATE.format(
                import_path=import_path,
                function_name=function_name,
                sql_path=sql_path,
                module_key=module_key,
                base_name=snake(Path(sql_path).stem),
                test_data_root=TEST_DATA_ROOT
            ))
        cfg[module_key] = {"csv_folder": f"{TEST_DATA_ROOT}/{module_key}"}
        print(f"Generated wrapper: {wrapper_path}")
        print(f"Generated test:    {test_path}")
    save_cfg(cfg)
    print(f"Updated config: {TEST_CFG}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-root", default=SQL_ROOT_DEFAULT, help="SQL root folder")
    parser.add_argument("--scan-all", action="store_true", help="Scan entire repo for .sql")
    args = parser.parse_args()
    generate(args.sql_root, args.scan_all)
