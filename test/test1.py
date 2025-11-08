import re

def parse_query(query):
    tokens = query.strip().split()

    if len(tokens) < 2:
        raise SyntaxError("Requête incomplète : il manque le mot-clé après 'CREATE'.")

    if tokens[0].upper() == "CREATE":
        if tokens[1].upper() == "TABLE":
            # Vérifie la syntaxe générale
            match = re.match(r"CREATE TABLE (\w+) \((.+)\)", query, re.IGNORECASE)
            if not match:
                # Détection d’erreurs spécifiques :
                if "(" not in query or ")" not in query:
                    raise SyntaxError("Erreur : les parenthèses autour des colonnes sont manquantes.")
                elif "," not in query and " " not in query.split("TABLE")[1].strip():
                    raise SyntaxError("Erreur : le nom de la table ou les colonnes sont absents.")
                else:
                    raise SyntaxError("Erreur de syntaxe proche de 'CREATE TABLE'.")

            table_name = match.group(1)
            values_col = match.group(2).split(',')
            columns = []

            for i, col in enumerate(values_col, start=1):
                part = col.strip().split()
                if len(part) < 1:
                    raise SyntaxError(f"Erreur dans la colonne {i}: colonne vide.")
                if len(part) == 1:
                    raise SyntaxError(f"Erreur dans la colonne {i}: type de donnée manquant pour '{part[0]}'.")
                
                col_name = part[0]
                col_type = part[1]
                constraints = part[2:] if len(part) > 2 else []
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "constraints": constraints
                })

            return {
                "type": "CREATE_TABLE",
                "table": table_name,
                "columns": columns
            }

        elif tokens[1].upper() == "DATABASE":
            if len(tokens) < 3:
                raise SyntaxError("Erreur : nom de la base manquant après 'CREATE DATABASE'.")
            db_name = tokens[2]
            if not re.match(r"^[A-Za-z_]\w*$", db_name):
                raise SyntaxError(f"Nom de base invalide : '{db_name}'.")
            return {
                "type": "CREATE_DATABASE",
                "database": db_name
            }

        else:
            raise SyntaxError(f"Erreur : mot-clé inattendu '{tokens[1]}'. Utilise TABLE ou DATABASE.")

    else:
        raise SyntaxError(f"Commande non reconnue : '{tokens[0]}'")
