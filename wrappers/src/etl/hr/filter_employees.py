from utils.sql_loader import load_sql

def run_filter_employees(spark):
    """Auto-generated wrapper to execute SQL: src/etl/hr/filter_employees.sql"""
    sql = load_sql(r"src/etl/hr/filter_employees.sql")
    return spark.sql(sql)
