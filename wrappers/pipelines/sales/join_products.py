from utils.sql_loader import load_sql

def run_join_products(spark):
    """Auto-generated wrapper to execute SQL: pipelines/sales/join_products.sql"""
    sql = load_sql(r"pipelines/sales/join_products.sql")
    return spark.sql(sql)
