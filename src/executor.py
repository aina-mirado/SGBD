import sys
from pathlib import Path
from typing import Optional



# ajoute la racine du projet au PYTHONPATH (permet d'importer models depuis src)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.models.databases import Database
from src.models.table import Table

from src.usefonctions import *

def executor(parsed: dict):
    if not parsed:
        print("no_parsed_input")
        return

    t = parsed.get("action") or parsed.get("type")

    if t == "CREATE_DATABASE":
        db_name = parsed.get("database_name")
        if_not_exists = bool(parsed.get("if_not_exists", False))
        db = Database(db_name)
        result = db.create_db(if_not_exists=if_not_exists)
        return result

    if t == "USE":
        db_name = parsed.get("argument")
        if not db_name:
            return {"success": False, "error": "no_database_name"}

        # vérifier que la DB existe
        existing = Database.list_databases_at()
        if db_name not in existing:
            return {"success": False, "error": "database_not_found", "database": db_name}
        clear_current_db()
        ok = set_current_db(db_name)
        if ok:
            return {"success": True, "database": db_name}
        else:
            return {"success": False, "error": "io_error"}

    if t == "SHOW" and parsed.get("argument").upper() == "DATABASES":
        dbs = Database.list_databases_at()
        return {"databases": dbs}            
    
    if t == "DROP_DATABASE":
        name = parsed.get("database_name")
        if_exists = bool(parsed.get("if_exists", False))
        db = Database(name)
        if not db.path.exists():
            if if_exists:
                res = {"action": "DROP_DATABASE", "database": name, "dropped": False, "skipped": True}
            else:
                res = {"action": "DROP_DATABASE", "database": name, "dropped": False, "error": "not_found"}
            return res
        ok = db.remove_db()
        if ok:
            if name == get_current_db():
                clear_current_db()
            res = {"action": "DROP_DATABASE", "database": name, "dropped": bool(ok)}
            return res
    
    if t == "DROP_TABLE":
        table_name = parsed.get("table_name") or parsed.get("argument")
        if not table_name:
            return {"dropped": False, "error": "no_table_name"}
        dbname = parsed.get("database") or get_current_db()
        if not dbname:
            return {"dropped": False, "error": "no_database_selected"}
        db = Database(dbname)
        if_exists = bool(parsed.get("if_exists", False))
        result = db.drop_table(table_name, if_exists=if_exists)
        return result
    
    if t == "CREATE_TABLE":
        dbname = get_current_db()
        if not dbname:
            res = {"action": "CREATE_TABLE", "created": False, "error": "no_database_selected"}
            return res

        db = Database(dbname)
        try:
            result = db.create_table(parsed)
        except Exception as e:
            result = {"action": "CREATE_TABLE", "created": False, "error": "exception", "detail": str(e)}

        return result
    
    if t == "SHOW" and parsed.get("argument").upper() == "TABLES" :        
        # dbname = check_current_db_selected()
        dbname = get_current_db()
        if not dbname:
            return {"error": "no_database_selected"}
        db = Database(dbname)
        tables = db.show_tables()
        return {"database": dbname, "tables": tables}
    
    if t == "DESCRIBE":
        table_name = parsed.get("argument")
        return Table.describe_table(table_name)
    
    if t == "INSERT":
        return Table.insert(parsed)
    
    return {"error": "unsupported_action", "action": t}

    