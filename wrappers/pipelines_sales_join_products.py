from utils.sql_loader import load_sql
def run_pipelines_sales_join_products(spark):
    sql = load_sql(r"pipelines/sales/join_products.sql")
    return spark.sql(sql)
