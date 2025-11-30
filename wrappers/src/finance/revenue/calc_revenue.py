from utils.sql_loader import load_sql

def run_calc_revenue(spark):
    """Auto-generated wrapper to execute SQL: src/finance/revenue/calc_revenue.sql"""
    sql = load_sql(r"src/finance/revenue/calc_revenue.sql")
    return spark.sql(sql)
