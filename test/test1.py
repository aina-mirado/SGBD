
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