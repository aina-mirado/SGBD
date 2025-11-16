import json
import shutil
from pathlib import Path
from typing import Optional, List, Dict, Any


class Database:
    # déclaration des attributs (privés)
    _name: str
    _base_path: Path
    _path: Path
    _rules_file: Path
    _tables: List[str]

    def __init__(self, name: str, base_path: Optional[str] = None):
        # attribution des valeurs
        self._name = name
        self._base_path = Path(base_path) if base_path else Path.cwd() / "Data"
        self._path = (self._base_path / self._name).resolve()
        self._rules_file = self._path / "informationTable.json"
        self._tables = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Path:
        return self._path

    @property
    def rules_file(self) -> Path:
        return self._rules_file

    def create_db(self, if_not_exists: bool = False) -> Dict[str, Any]:
        """
        Crée Data/<name> et un fichier informationTable.json.
        - if_not_exists True  : si la DB existe, ne rien faire et retourner created=False
        - if_not_exists False : si la DB existe, créer une nouvelle DB avec suffixe _1, _2, ...
        Retourne un dict avec au minimum {"created": bool, "name": <created_name>, ...}
        """
        try:
            self._base_path.mkdir(parents=True, exist_ok=True)

            if self._path.exists():
                if if_not_exists:
                    return {"created": False, "reason": "already_exists", "name": self._name}
                # générer un nouveau nom avec suffixe _N
                base = self._name
                suffix = 1
                while True:
                    candidate = f"{base}_{suffix}"
                    candidate_path = self._base_path / candidate
                    if not candidate_path.exists():
                        break
                    suffix += 1
                target_path = candidate_path
                created_name = candidate
            else:
                target_path = self._path
                created_name = self._name

            target_path.mkdir(parents=True, exist_ok=False)
            rules = {"relations": [], "tables": []}
            rules_file = target_path / "informationTable.json"
            with open(rules_file, "w", encoding="utf-8") as f:
                json.dump(rules, f, indent=2, ensure_ascii=False)

            # mettre à jour l'objet pour pointer vers la DB créée
            self._name = created_name
            self._path = target_path.resolve()
            self._rules_file = self._path / "informationTable.json"

            return {"created": True, "name": created_name, "path": str(self._path)}
        except Exception as e:
            return {"created": False, "error": str(e)}

    def remove_db(self) -> bool:
        try:
            if self._path.exists():
                shutil.rmtree(self._path)
            return True
        except Exception:
            return False

    def modify_db(self, new_name: str) -> bool:
        try:
            new_path = self._base_path / new_name
            if new_path.exists():
                return False
            self._path.rename(new_path)
            self._name = new_name
            self._path = new_path.resolve()
            self._rules_file = self._path / "informationTable.json"
            return True
        except Exception:
            return False

    def list_databases(self) -> List[str]:
        """
        Retourne la liste (triée) des noms de dossiers présents dans self._base_path.
        Si le dossier base_path n'existe pas, retourne [].
        """
        try:
            bp = self._base_path
            if not bp.exists():
                return []
            return sorted([p.name for p in bp.iterdir() if p.is_dir()])
        except Exception:
            return []

    @staticmethod
    def list_databases_at(base_path: Optional[str] = None) -> List[str]:
        """
        Méthode utilitaire : liste les dossiers dans base_path donné (ou ~/Data par défaut).
        Permet d'appeler sans créer d'instance.
        """
        try:
            bp = Path(base_path) if base_path else Path.cwd() / "Data"
            if not bp.exists():
                return []
            return sorted([p.name for p in bp.iterdir() if p.is_dir()])
        except Exception:
            return []

    def create_table(self, table_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ajoute la définition de la table dans informationTable.json ET initialise
        le stockage des données en créant <table_name>.json contenant {"rows": []}.
        Vérifie que la table n'existe pas déjà dans informationTable.json ou comme
        fichier de données avant de créer. Retourne un dict résultat.
        """
        name = table_def.get("table_name") or table_def.get("name")
        if not name:
            return {"created": False, "error": "no_table_name"}

        # assure le répertoire de la DB et le fichier de règles existent (au moins en mémoire)
        try:
            self._path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return {"created": False, "error": "cannot_create_db_dir", "detail": str(e)}

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

        # vérifie existence dans les règles
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
                    "column_name": fk.get("columns"),
                    "referenced_table": fk.get("referenced_table") ,
                    "referenced_column": fk.get("referenced_columns") ,
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

        # crée/initialise le fichier de données <table>.json (ne pas écraser s'il existe)
        table_file = self._path / f"{name}.json"
        if table_file.exists():
            return {"created": False, "error": "table_data_file_exists", "table": name}

        try:
            with open(table_file, "w", encoding="utf-8") as tf:
                json.dump({"rows": []}, tf, indent=2, ensure_ascii=False)
        except Exception as e:
            return {"created": False, "error": "cannot_create_table_file", "detail": str(e)}

        # ajoute l'entrée dans informationTable.json et sauvegarde
        tables.append(table_entry)
        try:
            self._rules_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._rules_file, "w", encoding="utf-8") as f:
                json.dump(rules, f, indent=2, ensure_ascii=False)
        except Exception as e:
            # rollback en mémoire
            tables.pop()
            return {"created": False, "error": "cannot_write_rules", "detail": str(e)}

        return {"created": True, "table": name, "table_file": str(table_file), "rules_file": str(self._rules_file)}

    def show_tables(self) -> List[str]:
        try:
            if not self._rules_file.exists():
                return []
            with open(self._rules_file, "r", encoding="utf-8") as f:
                rules = json.load(f)
            return [t.get("name") for t in rules.get("tables", []) if t.get("name")]
        except Exception:
            return []