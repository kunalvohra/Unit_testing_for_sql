import os

def load_sql(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
