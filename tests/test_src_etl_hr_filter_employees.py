import os
import pytest
from utils.sql_loader import load_sql
from utils.sql_table_parser import extract_tables_with_fullnames
from utils.data_loader import discover_table_parquet_info, resolve_parquet_for_case, load_csv_as_df
from utils.csv_schema_resolver import normalize_csv_df
from wrappers.src_etl_hr_filter_employees import run_src_etl_hr_filter_employees

SQL_PATH = r"src/etl/hr/filter_employees.sql"
MODULE_KEY = r"src/etl/hr/filter_employees"
MODULE_FOLDER = os.path.join('test_data', MODULE_KEY)

def _collect_tables():
    sql = load_sql(SQL_PATH)
    return [b for (_, b) in extract_tables_with_fullnames(sql)]

def _bundle_params():
    tables = _collect_tables()
    info = {t: discover_table_parquet_info(MODULE_FOLDER, t) for t in tables}
    case_ids = set(['default'])
    for t, ti in info.items(): case_ids.update(ti.get('cases', {}).keys())
    bundles = []
    for cid in sorted(case_ids):
        mapping, valid = {}, True
        for t, ti in info.items():
            p = resolve_parquet_for_case(ti, cid) or ti.get('default')
            if not p: valid = False; break
            mapping[t] = p
        if valid: bundles.append((cid, mapping))
    return bundles

@pytest.mark.parametrize('caseid, mapping', _bundle_params())
def test_src_etl_hr_filter_employees_bundle(spark, caseid, mapping):
    for table, path in mapping.items():
        assert os.path.exists(path), f'Missing CSV: {table}: {path}'
        df = load_csv_as_df(spark, path)
        df = normalize_csv_df(df, table_name=table)
        df.createOrReplaceTempView(table)
    out = run_{wrapper_name}(spark)
    assert out is not None
    _ = out.count()
