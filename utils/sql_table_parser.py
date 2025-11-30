import re

def extract_tables_with_fullnames(sql_text):
    # Extract tables referenced by FROM or JOIN
    pattern = r"(?:from|join)\s+([a-zA-Z0-9_.]+)"
    matches = re.findall(pattern, sql_text, flags=re.IGNORECASE)

    output = []
    for full in matches:
        base = full.split(".")[-1]
        output.append((full, base))
    return output
