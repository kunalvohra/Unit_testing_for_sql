from utils.sql_loader import load_sql
def run_src_etl_hr_filter_employees(spark):
    sql = load_sql(r"src/etl/hr/filter_employees.sql")
    return spark.sql(sql)
