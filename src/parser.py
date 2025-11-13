import re
from xml.etree.ElementTree import ParseError

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
        # Erreur: Syntaxe de base incorrecte
        print("Erreur de syntaxe INSERT INTO incorrecte.")
        return None

    match = re.match(r"INSERT\s+INTO\s+(\w+)\s*(\((.*?)\))?\s*VALUES\s*\((.+)\)", query, re.IGNORECASE)

    if not match:
        print("Erreur de syntaxe INSERT INTO. Vérifiez la structure.")
        return None

    table_name = match.group(1)
    # Les colonnes sont optionnelles, group(3) peut être None si pas de parenthèses de colonnes.
    columns_str = match.group(3)
    values_str = match.group(4)

    columns = [col.strip() for col in columns_str.split(',')] if columns_str else None
    
    values = [val.strip().strip("'\"") for val in values_str.split(',')]
    
    # Simple vérification de cohérence (basée sur les tokens/regex de base)
    if columns and len(columns) != len(values):
        print("Erreur: Le nombre de colonnes et de valeurs ne correspond pas.")
        return None

    return {
        "action": "INSERT",
        "table_name": table_name,
        "columns": columns,
        "values": values
    }

def parse_delete(query, tokens):
    if len(tokens) < 3 or tokens[1].upper() != "FROM":
        print("Erreur de syntaxe DELETE FROM incorrecte.")
        return None
    
    # Expression régulière pour capturer :
    # 1. Nom de la table (\w+)
    # 2. Le reste de la requête après le nom de la table, y compris WHERE facultatif
    match = re.match(r"DELETE\s+FROM\s+(\w+)\s*(.*)", query, re.IGNORECASE)
    
    if not match:
        print("Erreur de syntaxe DELETE FROM. Vérifiez le nom de la table.")
        return None

    table_name = match.group(1)
    where_clause = match.group(2).strip()

    condition = None
    if where_clause and where_clause.upper().startswith("WHERE"):
        # Extrait la condition après 'WHERE' (y compris l'espace)
        condition = where_clause[5:].strip()
    
    if not condition and len(tokens) > 3:
         print("Attention: DELETE sans clause WHERE. Toutes les lignes seront supprimées.")

    return {
        "action": "DELETE",
        "table_name": table_name,
        "condition": condition  # La chaîne de condition (ex: "age > 30")
    }
    
def parse_update(query, tokens):

    if len(tokens) < 4 or tokens[2].upper() != "SET":
        print("Erreur de syntaxe UPDATE incorrecte.")
        return None

    # Expression régulière pour capturer :up
    # 1. Nom de la table (\w+)
    # 2. La liste des assignments SET (col=val, ...) après SET (.*?)
    # 3. Le reste de la requête (y compris WHERE facultatif)
    match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$", query, re.IGNORECASE | re.DOTALL)
    
    if not match:
        print("Erreur de syntaxe UPDATE. Vérifiez la clause SET.")
        return None

    table_name = match.group(1)
    set_assignments_str = match.group(2)
    where_clause_str = match.group(3)

    assignments = {}
    
    # Analyse des assignments SET (ex: "col1 = val1, col2 = val2")
    try:
        for assignment in set_assignments_str.split(','):
            parts = [p.strip() for p in assignment.split('=', 1)]
            if len(parts) == 2:
                col_name = parts[0]
                col_value = parts[1].strip().strip("'\"") # Retire les guillemets simples/doubles pour l'exemple
                assignments[col_name] = col_value
            else:
                raise ValueError("Format d'assignation SET incorrect.")
    except ValueError as e:
        print(f"Erreur d'analyse SET: {e}")
        return None

    condition = where_clause_str.strip() if where_clause_str else None

    return {
        "action": "UPDATE",
        "table_name": table_name,
        "assignments": assignments,  # Dictionnaire {colonne: nouvelle_valeur}
        "condition": condition
    }

def parse_cmd(query, tokens):
    action = tokens[0].upper()

    if len(tokens) == 2:
        return {
            'action': action,
            'argument' : query.split()[1].strip(';')
        }
    if len(tokens) == 1:
        return {
            'action': action
        }
    return None

def parse_drop(query, tokens):
    """
    Supporte:
      - DROP DATABASE [IF EXISTS] name
      - DROP TABLE [IF EXISTS] [db.]table
    Retourne dict minimal ou None si non reconnu.
    """
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
        return {
            "action": "DROP_DATABASE",
              "database_name": name, 
              "if_exists": if_exists}
    # TABLE (possibilité db.table)
    if "." in name:
        db, tbl = name.split(".", 1)
    else:
        db, tbl = None, name
    return {
        "action": "DROP_TABLE", 
        "table_name": tbl, "database": db, 
        "if_exists": if_exists}
