#!/usr/bin/env python3
"""One-off migration helper: rewrite StarRocks `CREATE TABLE` statements in a
sql-tests suite into Iceberg-v3 form.

For each `CREATE TABLE name (col_list) <tail>;` it strips native storage
clauses from <tail> and appends/merges TBLPROPERTIES("format-version"="3").
`CREATE TABLE ... AS SELECT` is left untouched. Every change is printed for
human diff review; the record/verify gate is the real safety net.

Usage:  python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/<suite>/sql
"""
import re
import sys
from pathlib import Path

# Native clauses to remove from the options tail (case-insensitive).
NATIVE_CLAUSES = [
    r"\bDUPLICATE\s+KEY\s*\([^)]*\)",
    r"\bAGGREGATE\s+KEY\s*\([^)]*\)",
    r"\bUNIQUE\s+KEY\s*\([^)]*\)",
    r"\bPRIMARY\s+KEY\s*\([^)]*\)",
    r"\bDISTRIBUTED\s+BY\s+HASH\s*\([^)]*\)(\s+BUCKETS\s+\d+)?",
    r"\bDISTRIBUTED\s+BY\s+RANDOM(\s+BUCKETS\s+\d+)?",
    r"\bORDER\s+BY\s*\([^)]*\)",
    r"\bPROPERTIES\s*\([^)]*\)",
    r"\bENGINE\s*=\s*\w+",
]
FV = '"format-version" = "3"'


def find_col_list_end(s, open_idx):
    """Return index just past the matching ')' for the '(' at open_idx."""
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def rewrite_statement(stmt):
    """Rewrite one CREATE TABLE statement (without trailing ';'). Returns
    (new_stmt, changed: bool)."""
    m = re.search(r"create\s+table\s+(if\s+not\s+exists\s+)?", stmt, re.I)
    if not m:
        return stmt, False
    # Find first '(' after the table name; if 'AS SELECT' comes first -> CTAS.
    paren = stmt.find("(", m.end())
    as_sel = re.search(r"\bAS\b", stmt[m.end():], re.I)
    if paren == -1 or (as_sel and m.end() + as_sel.start() < paren):
        return stmt, False  # CTAS or no column list
    end = find_col_list_end(stmt, paren)
    if end == -1:
        return stmt, False
    head = stmt[:end]            # CREATE TABLE name (cols)
    tail = stmt[end:]            # storage clauses

    existing_tblprops = re.search(r"\bTBLPROPERTIES\s*\(([^)]*)\)", tail, re.I)
    for pat in NATIVE_CLAUSES:
        tail = re.sub(pat, "", tail, flags=re.I)
    tail = re.sub(r"\bTBLPROPERTIES\s*\([^)]*\)", "", tail, flags=re.I)
    tail = tail.strip()

    if existing_tblprops:
        inner = existing_tblprops.group(1).strip()
        if re.search(r'format-version', inner, re.I):
            merged = re.sub(r'"format-version"\s*=\s*"\d+"', FV, inner, flags=re.I)
        else:
            merged = (inner + ", " if inner else "") + FV
        props = f'TBLPROPERTIES ({merged})'
    else:
        props = f'TBLPROPERTIES ({FV})'

    new_stmt = f"{head}\n{props}"
    return new_stmt, True


def process_file(path):
    text = path.read_text()
    out = []
    changed = False
    # Split on ';' but keep statements; naive split is fine for these suites.
    parts = re.split(r";", text)
    for idx, part in enumerate(parts):
        if re.search(r"create\s+table", part, re.I):
            new, ch = rewrite_statement(part)
            if ch:
                changed = True
                print(f"  [{path.name}] rewrote a CREATE TABLE")
            out.append(new)
        else:
            out.append(part)
    if changed:
        path.write_text(";".join(out))
    return changed


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sql_dir = Path(sys.argv[1])
    n = 0
    for f in sorted(sql_dir.glob("*.sql")):
        if process_file(f):
            n += 1
    print(f"rewrote CREATE TABLE in {n} file(s) under {sql_dir}")


if __name__ == "__main__":
    main()
