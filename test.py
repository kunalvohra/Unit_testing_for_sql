"""
This script generates a full example project containing:
- SQL files in multiple source folders
- Corresponding input CSV test files (default + case)
- Expected output CSVs
- Final ZIP bundle

After execution, you will get:
    sql_framework_sample_input_bundle.zip
"""

import os
import shutil
import zipfile
from pathlib import Path

# ----------------------------
# 1. Setup root folder
# ----------------------------

# root = Path("")
# if root.exists():
#     shutil.rmtree(root)
# root.mkdir(parents=True, exist_ok=True)

# ----------------------------
# 2. SQL files in multiple source folders
# ----------------------------

sql_files = {
    "src/etl/hr/filter_employees.sql": """
SELECT emp_id, name, dept_id 
FROM employees 
WHERE active = 'Y';
""",
    "src/finance/revenue/calc_revenue.sql": """
SELECT c.customer_id, SUM(o.amount) AS total_amount 
FROM customers c 
JOIN orders o 
ON c.customer_id = o.customer_id 
GROUP BY c.customer_id;
""",
    "pipelines/sales/join_products.sql": """
SELECT s.sale_id, p.product_name 
FROM sales s 
JOIN products p 
ON s.product_id = p.product_id;
"""
}

# ----------------------------
# 3. Input CSV test data
# ----------------------------

csv_files = {
    # HR module
    "test_data/src/etl/hr/filter_employees/employees.csv":
        "emp_id,name,dept_id,active\n1,Alice,10,Y\n2,Bob,20,N\n3,Charlie,10,Y",

    "test_data/src/etl/hr/filter_employees/employees_case1.csv":
        "emp_id,name,dept_id,active\n4,David,30,Y",

    # Finance customers + orders
    "test_data/src/finance/revenue/calc_revenue/customers.csv":
        "customer_id,customer_name\n1,Alice\n2,Bob",

    "test_data/src/finance/revenue/calc_revenue/orders.csv":
        "order_id,customer_id,amount\n100,1,250\n101,1,100\n102,2,500",

    "test_data/src/finance/revenue/calc_revenue/orders_case1.csv":
        "order_id,customer_id,amount\n200,1,999",

    # Sales module
    "test_data/pipelines/sales/join_products/sales.csv":
        "sale_id,product_id\n10,100\n11,200",

    "test_data/pipelines/sales/join_products/products.csv":
        "product_id,product_name\n100,Widget\n200,Gadget",
}

# ----------------------------
# 4. Expected Outputs
# ----------------------------

expected_files = {
    "expected/src/etl/hr/filter_employees/output.csv":
        "emp_id,name,dept_id\n1,Alice,10\n3,Charlie,10",

    "expected/src/finance/revenue/calc_revenue/output.csv":
        "customer_id,total_amount\n1,350\n2,500",

    "expected/pipelines/sales/join_products/output.csv":
        "sale_id,product_name\n10,Widget\n11,Gadget",
}

# ----------------------------
# 5. Write files into project
# ----------------------------

def write_files(file_dict):
    for rel_path, content in file_dict.items():
        folder = os.path.dirname(rel_path)

        # 1. Create directory tree safely (nested dirs allowed)
        if folder:
            os.makedirs(folder, exist_ok=True)

        # 2. Write the file
        with open(rel_path, "w", encoding="utf-8") as f:
            f.write(content.strip() + "\n")

write_files(sql_files)
write_files(csv_files)
write_files(expected_files)


setx JAVA_HOME "C:\Users\vohrakb\Downloads\jdk-25_windows-x64_bin\jdk-25.0.1"

