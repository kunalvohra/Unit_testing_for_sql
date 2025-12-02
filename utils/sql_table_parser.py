import re

def extract_tables_with_fullnames(sql_text):
    pattern = r"(?:from|join)\\s+([a-zA-Z0-9_.]+)"
    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)
    tables = []
    for full in matches:
        base = full.split(".")[-1]
        tables.append((full, base))
    return tables
