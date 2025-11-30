from utils.sql_loader import load_sql
def run_src_finance_revenue_calc_revenue(spark):
    sql = load_sql(r"src/finance/revenue/calc_revenue.sql")
    return spark.sql(sql)
