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
    query = query.rstrip(";")
    query = query.strip()

    return query

def analyseSyntax(query):
    tokens = [token.strip().upper() for token in query.split()]

    if tokens[0] == "CREATE":
        if tokens[1] == "TABLE":
            match  = re.match(r"CREATE TABLE (\w+)\((.+)\)", query ,re.IGNORECASE)
            
            if not match:
                # raise SyntaxError("Syntaxe CREATE incorrecte")
                print("error du syntax")
            
            else:
                table_name = match.group(1)
                values_col = match.group(2).split(',')
                columns = []

                for col in values_col:
                    part = col.strip().split()
                    col_name = part[0]
                    col_type = part[1] if len(part) > 1 else "TEXT"
                    constraints = part[2:] if len(part) > 2 else []
                    columns.append({"name": col_name, "type":col_type, "constraints": constraints})

                return {
                        "type": "CREATE_TABLE",
                        "table_name": table_name,
                        "columns": columns
                    }
        
        if tokens[1] == "DATABASE":
            return tokens[2]
        else: 
            print(f"impossible de creer {tokens[1]}")
    


# if __name__ == "__main__":
#     query = "CREATE table etudiants (id INT PRIMARY KEY, nom TEXT, age INT);"
#     # query = "CREATE D mon_DB;"
#     parser(query)
#     try:
#         print(parser(query))
        

#     except ParseError as e:
#         print(f"ParseError: {e}")
