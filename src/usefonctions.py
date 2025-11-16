from pathlib import Path
from typing import Optional
import json

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
    Affichage court et générique des résultats d'executor.
    """
    if result is None:
        print("RESULT: None"); return
    if isinstance(result, bool):
        print("RESULT:", result); return
    if not isinstance(result, dict):
        print("RESULT:", result); return

    if result.get("error"):
        msg = result["error"]
        if result.get("detail"):
            msg += " - " + result["detail"]
        print("ERROR:", msg); return

    if result.get("created") is True:
        name = result.get("name") or result.get("table") or result.get("database")
        path = result.get("path")
        if name and path:
            print(f"Créé: {name} ({path})")
        elif name:
            print(f"Créé: {name}")
        else:
            print("Création réussie:", result)
        return

    if result.get("created") is False:
        print("Échec:", result.get("error", result)); return

    if "success" in result:
        if result["success"]:
            print("OK", result.get("database") or "")
        else:
            print("FAIL:", result.get("error", ""))
        return

    if "databases" in result:
        dbs = result.get("databases") or []
        print("Bases:", ", ".join(dbs) if dbs else "(aucune)"); return

    if "tables" in result:
        tbls = result.get("tables") or []
        print("Tables in", result.get("database") or "(unknown):", ", ".join(tbls) if tbls else "(aucune)")
        return

    if "dropped" in result:
        if result.get("dropped"):
            print("Supprimé:", result.get("database") or result.get("table"))
        else:
            if result.get("skipped"):
                print("Suppression ignorée (if_exists).")
            else:
                print("Suppression échouée:", result.get("error", ""))
        return

    print(json.dumps(result, ensure_ascii=False, indent=2))