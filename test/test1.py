
#ces fonction sont des backups

import json


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
        # récupère nom, type (avec params éventuels) et reste contraintes
        m = re.match(r"^\s*(\w+)\s+(\w+(?:\s*\([^)]*\))?)(.*)$", col_def, re.IGNORECASE | re.DOTALL)
        if not m:
            return None, None, []
        name = m.group(1)
        typ = m.group(2).strip()
        rest = m.group(3).strip()
        # split constraints en mots (garde tokens composés comme PRIMARY KEY)
        cons = [c.strip() for c in re.split(r'\s+(?=(?:PRIMARY|NOT|UNIQUE|DEFAULT|AUTO_INCREMENT|REFERENCES|CHECK|CONSTRAINT)\b)|\s+', rest) if c.strip()]
        return name, typ, cons

    columns = []
    constraints = []

    for definition in split_top_level(content_str):
        if not definition:
            continue
        up = definition.strip()

        # 1) Constraint table-level FOREIGN KEY (optionnel CONSTRAINT nom)
        fk_table = re.match(
            r"^(?:CONSTRAINT\s+(\w+)\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION))?",
            up, re.IGNORECASE | re.DOTALL
        )
        if fk_table:
            constraint_name = fk_table.group(1)
            local_cols = [c.strip() for c in fk_table.group(2).split(',')]
            ref_table = fk_table.group(3)
            ref_cols = [c.strip() for c in fk_table.group(4).split(',')]
            on_delete = fk_table.group(5).replace(" ", "_").upper() if fk_table.group(5) else "NO_ACTION"
            constraints.append({
                "type": "FOREIGN_KEY",
                "name": constraint_name,
                "columns": local_cols,
                "referenced_table": ref_table,
                "referenced_columns": ref_cols,
                "on_delete": on_delete
            })
            continue

        # 2) Colonne possible avec REFERENCES inline
        col_name, col_type, col_cons = parse_column_def(up)
        if col_name is None:
            # fallback : prendre tout comme colonne brute
            columns.append({"name": up, "type": None, "constraints": []})
            continue

        # cherche un REFERENCES inline dans la définition
        fk_inline = re.search(r"REFERENCES\s+(\w+)\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION))?",
                              up, re.IGNORECASE | re.DOTALL)
        if fk_inline:
            ref_table = fk_inline.group(1)
            ref_cols = [c.strip() for c in fk_inline.group(2).split(',')]
            on_delete = fk_inline.group(3).replace(" ", "_").upper() if fk_inline.group(3) else "NO_ACTION"
            # ajoute la colonne et la contrainte correspondante
            columns.append({
                "name": col_name,
                "type": col_type,
                "constraints": [c.upper() for c in col_cons if c]
            })
            constraints.append({
                "type": "FOREIGN_KEY",
                "columns": [col_name],
                "referenced_table": ref_table,
                "referenced_columns": ref_cols,
                "on_delete": on_delete
            })
            continue

        # sinon colonne normale
        columns.append({
            "name": col_name,
            "type": col_type,
            "constraints": [c.upper() for c in col_cons if c]
        })

    return {
        "action": "CREATE_TABLE",
        "table_name": table_name,
        "columns": columns,
        "constraints": constraints
    }


def create_table(self, table_def: Dict[str, Any]) -> Dict[str, Any]:

        name = table_def.get("table_name") or table_def.get("name")
        if not name:
            return {"created": False, "error": "no_table_name"}

        # charge ou initialise le fichier de règles
        try:
            if self._rules_file.exists():
                with open(self._rules_file, "r", encoding="utf-8") as f:
                    rules = json.load(f)
            else:
                rules = {"relations": [], "tables": []}
        except Exception as e:
            return {"created": False, "error": "cannot_read_rules", "detail": str(e)}

        tables = rules.setdefault("tables", [])

        # vérifie existence
        for t in tables:
            if t.get("name") == name:
                return {"created": False, "error": "table_exists", "table": name}

        # prépare columns et primary_keys
        cols = table_def.get("columns", []) or []
        cols_meta = []
        primary_keys: List[str] = []
        for c in cols:
            cname = c.get("name")
            ctype = c.get("type")
            ccons = c.get("constraints", []) or []
            upper_cons = [tok.upper() for tok in ccons]
            if any("PRIMARY" in tok for tok in upper_cons):
                primary_keys.append(cname)
            cols_meta.append({"name": cname, "type": ctype, "constraints": ccons[:]})

        # prépare foreign_keys depuis table_def["constraints"]
        fk_meta: List[Dict[str, Any]] = []
        for fk in table_def.get("constraints", []) or []:
            if (fk.get("type") or "").upper() == "FOREIGN_KEY":
                entry = {
                    "column_name": fk.get("column_name"),
                    "referenced_table": fk.get("referenced_table") or fk.get("references_table") or fk.get("references"),
                    "referenced_column": fk.get("referenced_column") or fk.get("references_column"),
                }
                if fk.get("on_delete"):
                    entry["on_delete"] = fk.get("on_delete").upper()
                if fk.get("on_update"):
                    entry["on_update"] = fk.get("on_update").upper()
                fk_meta.append(entry)

        table_entry = {
            "name": name,
            "columns": cols_meta,
            "primary_keys": primary_keys,
            "foreign_keys": fk_meta
        }

        # ajoute et sauvegarde
        tables.append(table_entry)
        try:
            self._rules_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._rules_file, "w", encoding="utf-8") as f:
                json.dump(rules, f, indent=2, ensure_ascii=False)
        except Exception as e:
            # rollback en mémoire
            tables.pop()
            return {"created": False, "error": "cannot_write_rules", "detail": str(e)}

        return {"created": True, "table": name}


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
