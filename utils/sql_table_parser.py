import sqlparse
import re

def _clean_identifier(token: str) -> str:
    token = token.strip()
    token = token.strip('`"[]')
    if '.' in token:
        return token.split('.')[-1]
    return token

def extract_tables_with_fullnames(sql_text: str):
    if not sql_text:
        return []
    normalized = sqlparse.format(sql_text, keyword_case='upper', strip_comments=True)
    pattern = re.compile(r'\b(?:FROM|JOIN|INTO)\b\s+([`"\[]?[A-Za-z0-9_.]+[`"\]]?)', re.IGNORECASE)
    matches = pattern.findall(normalized)
    seen = set()
    out = []
    for m in matches:
        full = m.strip('`"[]')
        base = _clean_identifier(full)
        if base.upper() in ('SELECT', 'VALUES', 'WITH'):
            continue
        if (full, base) not in seen:
            seen.add((full, base))
            out.append((full, base))
    return out
