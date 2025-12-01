from pathlib import Path
from typing import Optional
import json
from tabulate import tabulate
import pandas as pd


# fichier pour persister la DB courante
_CURRENT_DB_FILE = Path(__file__).resolve().parent.parent / "Data" / ".current_db"
_current_db_cache: Optional[str] = None

def _read_current_db_file() -> Optional[str]:
    """
    Lit et met en cache le nom de la DB courante. Retourne None si absent.
    """
    global _current_db_cache
    try:
        if _current_db_cache is not None:
            return _current_db_cache
        if _CURRENT_DB_FILE.exists():
            val = _CURRENT_DB_FILE.read_text(encoding="utf-8").strip()
            _current_db_cache = val or None
            return _current_db_cache
    except Exception:
        return None
    return None

def _write_current_db_file(name: Optional[str]) -> bool:
    """
    Écrit (ou supprime si name is None) le fichier .current_db et met à jour le cache.
    """
    global _current_db_cache
    try:
        _CURRENT_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        if name is None:
            if _CURRENT_DB_FILE.exists():
                _CURRENT_DB_FILE.unlink()
            _current_db_cache = None
            return True
        _CURRENT_DB_FILE.write_text(str(name), encoding="utf-8")
        _current_db_cache = str(name)
        return True
    except Exception:
        return False

def get_current_db(force_reload: bool = False) -> Optional[str]:
    """
    Retourne le nom de la DB courante. Si force_reload True, bypass le cache et relit le fichier.
    """
    global _current_db_cache
    if force_reload:
        _current_db_cache = None
    return _read_current_db_file()

def set_current_db(name: str) -> bool:
    """Définit la DB courante (persiste et met à jour cache)."""
    return _write_current_db_file(name)

def clear_current_db() -> bool:
    """Supprime la sélection de DB courante."""
    return _write_current_db_file(None)

def showResult(result):
    """
    Affiche de façon lisible les retours d'executor/méthodes.
    - Pour les résultats contenant des lignes/tabular ('rows' or list of dicts) affiche un tableau.
    - Pour listes simples ('databases','tables') affiche une colonne.
    - Gère messages simples (created/inserted/dropped/error).
    """


    def _print_table(rows, headers=None):
        if not rows:
            print("(aucune ligne)")
            return
        # rows: list of dicts or list of lists
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            headers = headers or list(rows[0].keys())
            table_rows = [[_fmt(r.get(h)) for h in headers] for r in rows]
        else:
            table_rows = rows
        if tabulate:
            print(tabulate(table_rows, headers=headers, tablefmt="github"))
        elif pd:
            try:
                df = pd.DataFrame(table_rows, columns=headers) if headers else pd.DataFrame(table_rows)
                print(df.to_string(index=False))
            except Exception:
                print(json.dumps(table_rows, ensure_ascii=False, indent=2))
        else:
            # simple fallback
            if headers:
                print(" | ".join(headers))
                print("-" * (3 * len(headers) + sum(len(h) for h in headers)))
            for row in table_rows:
                if isinstance(row, (list, tuple)):
                    print(" | ".join(str(x) for x in row))
                else:
                    print(row)

    def _fmt(v):
        if v is None:
            return "NULL"
        if isinstance(v, float):
            return repr(v)
        return str(v)

    if result is None:
        print("RESULT: None"); return

    if isinstance(result, bool):
        print("RESULT:", result); return

    # list or tuple of dicts -> table
    if isinstance(result, (list, tuple)):
        if not result:
            print("(vide)"); return
        if isinstance(result[0], dict):
            _print_table(result)
            return
        print(result); return

    if not isinstance(result, dict):
        print(result); return

    # error first
    if result.get("error"):
        msg = result.get("error")
        if result.get("detail"):
            msg += " - " + str(result["detail"])
        print("ERROR:", msg)
        return

    # SELECT-like result with rows
    if result.get("action") == "SELECT" or "rows" in result:
        rows = result.get("rows") or []
        cols = result.get("columns")
        _print_table(rows, headers=cols)
        if "count" in result:
            print(f"\nCount: {result.get('count')}")
        return

    # show databases / tables lists
    if "databases" in result:
        rows = [{"database": d} for d in (result.get("databases") or [])]
        _print_table(rows, headers=["database"])
        return
    if "tables" in result:
        rows = [{"table": d} for d in (result.get("tables") or [])]
        _print_table(rows, headers=["table"])
        return

    # DESCRIBE / describe_table: table metadata
    if result.get("name") and result.get("columns") and isinstance(result.get("columns"), list):
        print(f"Table: {result.get('name')}")
        cols = []
        for c in result.get("columns"):
            cons = c.get("constraints")
            if isinstance(cons, list):
                cons_s = ", ".join(str(x) for x in cons) if cons else ""
            else:
                cons_s = str(cons or "")
            cols.append({"name": c.get("name"), "type": c.get("type"), "constraints": cons_s})
        _print_table(cols, headers=["name", "type", "constraints"])
        if result.get("primary_keys"):
            print("Primary keys:", ", ".join(result.get("primary_keys") or []))
        if result.get("foreign_keys"):
            print("Foreign keys:")
            _print_table(result.get("foreign_keys"))
        return

    # CREATE / DROP / INSERT simple messages
    if result.get("created") is True:
        name = result.get("table") or result.get("name") or result.get("database")
        print("Créé:", name); return
    if result.get("created") is False:
        print("Création échouée:", result.get("error", "")); return

    if result.get("inserted") is True:
        row = result.get("row")
        if isinstance(row, dict):
            _print_table([row], headers=list(row.keys()))
        else:
            print("Inserted:", row)
        return
    if result.get("inserted") is False:
        print("Insertion échouée:", result.get("error", "")); return

    if "dropped" in result:
        if result.get("dropped"):
            print("Supprimé:", result.get("table") or result.get("database"))
        else:
            if result.get("skipped"):
                print("Suppression ignorée (if_exists).")
            else:
                print("Suppression échouée:", result.get("error", ""))
        return

    # fallback: pretty JSON
    print(json.dumps(result, ensure_ascii=False, indent=2))
