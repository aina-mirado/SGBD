import re
from xml.etree.ElementTree import ParseError
from typing import List, Dict, Optional, Tuple, Any

def parser(query):
    query_clean = normalize_query(query)
    parsed = analyseSyntax(query_clean)
    return parsed

def normalize_query(query: str) -> str:
    query = " ".join(query.split())
    query = re.sub(r"\s+\(", "(", query)
    query = re.sub(r"\s+\)", ")", query)
    query = re.sub(r"\s+,", ",", query)
    query = query.strip()
    return query

# ----------------- validation utilities -----------------
def is_valid_data_type(type_str: str) -> bool:
    if not type_str or not isinstance(type_str, str):
        return False
    t = type_str.strip().upper()
    if t in ("INT", "FLOAT", "TEXT", "TIMESTAMP"):
        return True
    if re.fullmatch(r"VARCHAR\s*\(\s*\d+\s*\)", t):
        return True
    return False

def normalize_data_type(type_str: str) -> Optional[str]:
    if not isinstance(type_str, str):
        return None
    t = type_str.strip().upper()
    m = re.fullmatch(r"VARCHAR\s*\(\s*(\d+)\s*\)", t)
    if m:
        return f"VARCHAR({int(m.group(1))})"
    if t in ("INT", "FLOAT", "TEXT", "TIMESTAMP"):
        return t
    return None

def normalize_constraints(tokens: Optional[List[str]]) -> Tuple[bool, Any]:
    if not tokens:
        return True, []
    if not isinstance(tokens, list):
        return False, "constraints must be a list"
    res = []
    i = 0
    n = len(tokens)
    while i < n:
        tok = str(tokens[i]).upper()
        if tok == "PRIMARY":
            if i + 1 < n and str(tokens[i + 1]).upper() == "KEY":
                res.append("PRIMARY_KEY"); i += 2; continue
            res.append("PRIMARY_KEY"); i += 1; continue
        if tok == "NOT":
            if i + 1 < n and str(tokens[i + 1]).upper() == "NULL":
                res.append("NOT_NULL"); i += 2; continue
            return False, "INVALID_CONSTRAINT_NEAR_NOT"
        if tok in ("AUTO_INCREMENT", "UNIQUE"):
            res.append(tok); i += 1; continue
        if tok == "DEFAULT":
            if i + 1 < n:
                val = tokens[i + 1]
                res.append({"DEFAULT": val}); i += 2; continue
            return False, "DEFAULT without value"
        if tok.startswith("DEFAULT="):
            val = tok.split("=", 1)[1]
            res.append({"DEFAULT": val}); i += 1; continue
        if tok in ("NOT_NULL", "PRIMARY_KEY"):
            res.append(tok); i += 1; continue
        return False, f"UNKNOWN_CONSTRAINT_TOKEN:{tok}"
    return True, res

def validate_type_and_constraints(type_str: str, constraints: Optional[List[str]]) -> Tuple[bool, Optional[str], Optional[List]]:
    nt = normalize_data_type(type_str)
    if nt is None:
        return False, f"INVALID_TYPE:{type_str}", None
    ok, nc = normalize_constraints(constraints)
    if not ok:
        return False, nc if isinstance(nc, str) else "INVALID_CONSTRAINTS", None
    return True, None, nc

# ----------------- main parser -----------------
def analyseSyntax(query):
    tokens = [token.strip().upper() for token in query.split()]

    if len(tokens) <= 2 :
        return parse_cmd(query,tokens)
    
    if tokens[0] == "CREATE":
        if tokens[1] == "TABLE":
            return parse_create_table(query, tokens)        
        if tokens[1] == "DATABASE":
            return parse_create_database(query, tokens)
        else: 
            print(f"creation impossible")
    
    if tokens[0] == "INSERT":
        return parse_insert(query, tokens)
    
    if tokens[0] == "DELETE":
        return parse_delete(query, tokens)
    
    if tokens[0] == "UPDATE":
        return parse_update(query, tokens)
    
    if tokens[0] == "DROP":
        return parse_drop(query, tokens)
    
    if tokens[0] == "SELECT":
        return parse_select(query, tokens)

def parse_create_table(query, tokens):
    if len(tokens) < 3 or tokens[1].upper() != "TABLE":
        print("Erreur de syntaxe CREATE TABLE incorrecte.")
        return None

    match = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", query, re.IGNORECASE | re.DOTALL)
    if not match:
        print("Erreur de syntaxe CREATE TABLE. Vérifiez la structure.")
        return None

    table_name = match.group(1)
    content_str = match.group(2).strip()

    def split_top_level(s: str):
        parts = []
        cur = []
        depth = 0
        in_single = False
        in_double = False
        for ch in s:
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == '(' and not in_single and not in_double:
                depth += 1
            elif ch == ')' and not in_single and not in_double:
                depth -= 1
            if ch == ',' and depth == 0 and not in_single and not in_double:
                parts.append(''.join(cur).strip())
                cur = []
            else:
                cur.append(ch)
        if cur:
            parts.append(''.join(cur).strip())
        return [p for p in parts if p]

    def parse_column_def(col_def: str):
        m = re.match(r"^\s*(\w+)\s+(\w+(?:\s*\([^)]*\))?)(.*)$", col_def, re.IGNORECASE | re.DOTALL)
        if not m:
            return None, None, []
        name = m.group(1)
        typ = m.group(2).strip()
        rest = m.group(3).strip()
        cons = [c.strip() for c in re.split(r'\s+(?=(?:PRIMARY|NOT|UNIQUE|DEFAULT|AUTO_INCREMENT|REFERENCES|CHECK|CONSTRAINT)\b)|\s+', rest) if c.strip()]
        return name, typ, cons

    columns = []
    constraints = []

    for definition in split_top_level(content_str):
        if not definition:
            continue
        up = definition.strip()

        fk_table = re.match(
            r"^(?:CONSTRAINT\s+(\w+)\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^)]+)\)",
            up, re.IGNORECASE | re.DOTALL
        )
        if fk_table:
            constraint_name = fk_table.group(1)
            local_cols = [c.strip() for c in fk_table.group(2).split(',')]
            ref_table = fk_table.group(3)
            ref_cols = [c.strip() for c in fk_table.group(4).split(',')]

            on_delete_m = re.search(r"ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", up, re.IGNORECASE)
            on_update_m = re.search(r"ON\s+UPDATE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", up, re.IGNORECASE)
            on_delete = on_delete_m.group(1).upper().replace(" ", "_") if on_delete_m else None
            on_update = on_update_m.group(1).upper().replace(" ", "_") if on_update_m else None

            entry = {
                "type": "FOREIGN_KEY",
                "name": constraint_name,
                "columns": local_cols,
                "referenced_table": ref_table,
                "referenced_columns": ref_cols,
            }
            if on_delete:
                entry["on_delete"] = on_delete
            if on_update:
                entry["on_update"] = on_update

            constraints.append(entry)
            continue

        col_name, col_type, col_cons = parse_column_def(up)
        if col_name is None:
            columns.append({"name": up, "type": None, "constraints": []})
            continue

        # validate type & constraints via shared utilities
        ok, err, normalized_cons = validate_type_and_constraints(col_type, col_cons)
        if not ok:
            print(f"Erreur type/constraint pour colonne '{col_name}': {err}")
            return None
        norm_type = normalize_data_type(col_type)

        fk_inline = re.search(r"REFERENCES\s+(\w+)\s*\(([^)]+)\)", up, re.IGNORECASE | re.DOTALL)
        if fk_inline:
            ref_table = fk_inline.group(1)
            ref_cols = [c.strip() for c in fk_inline.group(2).split(',')]

            on_delete_m = re.search(r"ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", up, re.IGNORECASE)
            on_update_m = re.search(r"ON\s+UPDATE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)", up, re.IGNORECASE)
            on_delete = on_delete_m.group(1).upper().replace(" ", "_") if on_delete_m else None
            on_update = on_update_m.group(1).upper().replace(" ", "_") if on_update_m else None

            columns.append({
                "name": col_name,
                "type": norm_type,
                "constraints": normalized_cons
            })
            fk_entry = {
                "type": "FOREIGN_KEY",
                "columns": [col_name],
                "referenced_table": ref_table,
                "referenced_columns": ref_cols,
            }
            if on_delete:
                fk_entry["on_delete"] = on_delete
            if on_update:
                fk_entry["on_update"] = on_update
            constraints.append(fk_entry)
            continue

        columns.append({
            "name": col_name,
            "type": norm_type,
            "constraints": normalized_cons
        })

    return {
        "action": "CREATE_TABLE",
        "table_name": table_name,
        "columns": columns,
        "constraints": constraints
    }

def parse_create_database(query, tokens):
    if len(tokens) < 3 or tokens[1].upper() != "DATABASE":
        print("Erreur de syntaxe CREATE DATABASE incorrecte.")
        return None

    match = re.match(r"CREATE\s+DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)", query, re.IGNORECASE)
    if not match:
        print("Erreur de syntaxe CREATE DATABASE. Vérifiez la structure.")
        return None

    if_not_exists = bool(match.group(1))
    db_name = match.group(2)

    return {
        "action": "CREATE_DATABASE",
        "database_name": db_name,
        "if_not_exists": if_not_exists
    }

def parse_insert(query, tokens):
    if len(tokens) < 5 or tokens[1].upper() != "INTO":
        print("Erreur de syntaxe INSERT INTO incorrecte.")
        return None

    match = re.match(r"INSERT\s+INTO\s+(\w+)\s*(\((.*?)\))?\s*VALUES\s*\((.+)\)", query, re.IGNORECASE)

    if not match:
        print("Erreur de syntaxe INSERT INTO. Vérifiez la structure.")
        return None

    table_name = match.group(1)
    columns_str = match.group(3)
    values_str = match.group(4)

    columns = [col.strip() for col in columns_str.split(',')] if columns_str else None
    values = [val.strip().strip("'\"") for val in values_str.split(',')]

    if columns and len(columns) != len(values):
        print("Erreur: Le nombre de colonnes et de valeurs ne correspond pas.")
        return None

    return {
        "action": "INSERT",
        "table_name": table_name,
        "columns": columns,
        "values": values
    }

def parse_cmd(query, tokens):
    action = tokens[0].upper()
    if len(tokens) == 2:
        return {'action': action, 'argument' : query.split()[1].strip(';')}
    if len(tokens) == 1:
        return {'action': action}
    return None

def parse_drop(query, tokens):
    m = re.match(
        r"DROP\s+(DATABASE|TABLE)\s+(IF\s+EXISTS\s+)?(`[^`]+`|'[^']+'|\"[^\"]+\"|[\w\.]+)\s*;?$",
        query, re.IGNORECASE
    )
    if not m:
        return None
    kind = m.group(1).upper()
    if_exists = bool(m.group(2))
    name = m.group(3)
    if name and (name[0] == name[-1]) and name[0] in ("'", '"', '`'):
        name = name[1:-1]

    if kind == "DATABASE":
        return {"action": "DROP_DATABASE", "database_name": name, "if_exists": if_exists}
    if "." in name:
        db, tbl = name.split(".", 1)
    else:
        db, tbl = None, name
    return {"action": "DROP_TABLE", "table_name": tbl, "database": db, "if_exists": if_exists}

def parse_select(query, tokens):
    """
    Supporte une syntaxe proche de MySQL :
      SELECT [DISTINCT] select_list
      FROM table_expr
      [WHERE ...]
      [GROUP BY col[, ...]]
      [HAVING ...]
      [ORDER BY col [ASC|DESC][, ...]]
      [LIMIT [offset,] count]
    Renvoie un dict avec clés: action, distinct, columns, from, where, group_by, having, order_by, limit
    (where/having sont des chaînes brutes pour évaluation ultérieure).
    """
    q = query.strip().rstrip(';')

    # helper to split commas au niveau top (ne pas séparer dans les parenthèses)
    def split_top_commas(s: str):
        parts, cur, depth, in_s, in_d = [], [], 0, False, False
        for ch in s:
            if ch == "'" and not in_d: in_s = not in_s
            elif ch == '"' and not in_s: in_d = not in_d
            elif ch == '(' and not in_s and not in_d: depth += 1
            elif ch == ')' and not in_s and not in_d and depth: depth -= 1
            if ch == ',' and depth == 0 and not in_s and not in_d:
                parts.append(''.join(cur).strip()); cur = []
            else:
                cur.append(ch)
        if cur: parts.append(''.join(cur).strip())
        return [p for p in parts if p]

    # pattern: capture select-list and from until next clause (WHERE/GROUP/HAVING/ORDER/LIMIT/end)
    rx = re.compile(
        r"""SELECT\s+(DISTINCT\s+)?(?P<select>.+?)\s+FROM\s+(?P<from>.+?)
            (?:\s+WHERE\s+(?P<where>.+?))?
            (?:\s+GROUP\s+BY\s+(?P<group>.+?))?
            (?:\s+HAVING\s+(?P<having>.+?))?
            (?:\s+ORDER\s+BY\s+(?P<order>.+?))?
            (?:\s+LIMIT\s+(?P<limit>.+?))?$
        """, re.IGNORECASE | re.DOTALL | re.VERBOSE
    )

    m = rx.match(q)
    if not m:
        print("Erreur de syntaxe SELECT.")
        return None

    distinct = bool(m.group(1))
    sel = m.group('select').strip()
    frm = m.group('from').strip()
    where = m.group('where').strip() if m.group('where') else None
    grp = m.group('group').strip() if m.group('group') else None
    having = m.group('having').strip() if m.group('having') else None
    order = m.group('order').strip() if m.group('order') else None
    limit = m.group('limit').strip() if m.group('limit') else None

    columns = split_top_commas(sel) if sel != '*' else ['*']

    # FROM: tenter de séparer par virgules au top-level, garder JOINs tels quels
    # si ' JOIN ' présent, on retourne la chaîne complète pour traitement par executor
    from_tables = None
    if re.search(r"\s+JOIN\s+", frm, re.IGNORECASE):
        from_tables = frm  # leave complex FROM (joins) as string
    else:
        from_tables = [t.strip() for t in split_top_commas(frm)]

    group_by = [g.strip() for g in split_top_commas(grp)] if grp else []
    order_by = []
    if order:
        for part in split_top_commas(order):
            p = part.strip()
            ps = p.split()
            col = ps[0]
            dir = ps[1].upper() if len(ps) > 1 and ps[1].upper() in ("ASC", "DESC") else "ASC"
            order_by.append({"column": col, "dir": dir})

    limit_parsed = None
    if limit:
        # MySQL: LIMIT count OR LIMIT offset,count
        parts = [x.strip() for x in limit.split(',')]
        try:
            if len(parts) == 1:
                limit_parsed = {"count": int(parts[0])}
            elif len(parts) == 2:
                limit_parsed = {"offset": int(parts[0]), "count": int(parts[1])}
        except Exception:
            limit_parsed = None

    return {
        "action": "SELECT",
        "distinct": distinct,
        "columns": columns,
        "from": from_tables,
        "where": where,
        "group_by": group_by,
        "having": having,
        "order_by": order_by,
        "limit": limit_parsed
    }

def parse_delete(query, tokens):
    """
    Supporte:
      DELETE FROM table [WHERE condition];
    condition est optionnelle (si absent, supprime tout).
    """
    if len(tokens) < 3 or tokens[1].upper() != "FROM":
        print("Erreur de syntaxe DELETE FROM incorrecte.")
        return None
    
    match = re.match(r"DELETE\s+FROM\s+(\w+)\s*(.*)", query, re.IGNORECASE)
    if not match:
        print("Erreur de syntaxe DELETE FROM. Vérifiez le nom de la table.")
        return None

    table_name = match.group(1)
    rest = match.group(2).strip().rstrip(';')

    condition = None
    if rest and rest.upper().startswith("WHERE"):
        condition = rest[5:].strip()
        if not condition:
            print("Attention: WHERE vide. Aucune condition spécifiée.")
    elif rest:
        print(f"Attention: texte ignoré après table: {rest}")
    
    if not condition:
        print("Attention: DELETE sans clause WHERE. Toutes les lignes seront supprimées.")

    return {
        "action": "DELETE",
        "table_name": table_name,
        "condition": condition
    }

def parse_update(query, tokens):
    """
    Supporte:
      UPDATE table SET col1=val1, col2=val2 [WHERE condition];
    condition est optionnelle.
    """
    if len(tokens) < 4 or tokens[2].upper() != "SET":
        print("Erreur de syntaxe UPDATE incorrecte.")
        return None

    match = re.match(
        r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$",
        query, re.IGNORECASE | re.DOTALL
    )
    if not match:
        print("Erreur de syntaxe UPDATE. Vérifiez la clause SET.")
        return None

    table_name = match.group(1)
    set_assignments_str = match.group(2).strip()
    where_clause_str = match.group(3)

    assignments = {}
    try:
        for assignment in set_assignments_str.split(','):
            parts = [p.strip() for p in assignment.split('=', 1)]
            if len(parts) == 2:
                col_name = parts[0]
                col_value = parts[1].strip().rstrip(';').strip("'\"")
                assignments[col_name] = col_value
            else:
                raise ValueError("Format d'assignation SET incorrect.")
    except ValueError as e:
        print(f"Erreur d'analyse SET: {e}")
        return None

    condition = where_clause_str.strip().rstrip(';') if where_clause_str else None

    return {
        "action": "UPDATE",
        "table_name": table_name,
        "assignments": assignments,
        "condition": condition
    }
