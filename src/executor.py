import sys
from pathlib import Path
from typing import Optional

# ajoute la racine du projet au PYTHONPATH (permet d'importer models depuis src)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.databases import Database

# fichier pour persister la DB courante
_CURRENT_DB_FILE = Path(__file__).resolve().parent.parent / "Data" / ".current_db"
_current_db_cache: Optional[str] = None

def _read_current_db_file() -> Optional[str]:
    global _current_db_cache
    if _current_db_cache is not None:
        return _current_db_cache
    try:
        if _CURRENT_DB_FILE.exists():
            val = _CURRENT_DB_FILE.read_text(encoding="utf-8").strip()
            _current_db_cache = val or None
            return _current_db_cache
    except Exception:
        return None
    return None

def _write_current_db_file(name: Optional[str]) -> bool:
    global _current_db_cache
    try:
        _CURRENT_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        if name is None:
            if _CURRENT_DB_FILE.exists():
                _CURRENT_DB_FILE.unlink()
            _current_db_cache = None
            return True
        _CURRENT_DB_FILE.write_text(name, encoding="utf-8")
        _current_db_cache = name
        return True
    except Exception:
        return False

def get_current_db() -> Optional[str]:
    return _read_current_db_file()

def set_current_db(name: str) -> bool:
    return _write_current_db_file(name)

def clear_current_db() -> bool:
    return _write_current_db_file(None)



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
        res = {"action": "DROP_DATABASE", "database": name, "dropped": bool(ok)}
        return res
    
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
    
    if t == "SHOW" and parsed.get("argument").upper() == "TABLES":
        # parsed may include "database" (FROM/IN); fallback to current DB
        dbname = get_current_db()
        if not dbname:
            return {"error": "no_database_selected"}
        db = Database(dbname)
        tables = db.show_tables()
        return {"tables": tables, "database": dbname}
    
    
    
    return {"error": "unsupported_action", "action": t}

    
# if __name__ == "__main__":
    # test rapide
    # parsed_create = {"action": "CREATE_DATABASE", "database_name": "nomDB", "if_not_exists": True}
    # main(parsed_create)
    # parsed_use = {"action": "USE", "database_name": "nomDB"}
    # main(parsed_use)
    # print("current:", get_current_db())
    # parsed_show = {"action": "SHOW_DATABASES"}
    # main(parsed_show)

    # parsed_drop = {"action": "DROP_DATABASE", "database_name": "nomDB", "if_exists": True}
    # main(parsed_drop)