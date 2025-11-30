import re

def extract_simple_column_set(sql_text: str):
    if not sql_text:
        return set()
    tokens = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', sql_text)
    keywords = set(['SELECT','FROM','WHERE','JOIN','ON','AS','AND','OR','IN','GROUP','BY','ORDER','LIMIT','CASE','WHEN','THEN','ELSE'])
    cols = set(t for t in tokens if t.upper() not in keywords)
    return cols
