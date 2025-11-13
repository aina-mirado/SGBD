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
        self._rules_file = self._path / "relationRules.json"
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
        Crée Data/<name> et un fichier relationRules.json.
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
            rules_file = target_path / "relationRules.json"
            with open(rules_file, "w", encoding="utf-8") as f:
                json.dump(rules, f, indent=2, ensure_ascii=False)

            # mettre à jour l'objet pour pointer vers la DB créée
            self._name = created_name
            self._path = target_path.resolve()
            self._rules_file = self._path / "relationRules.json"

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
            self._rules_file = self._path / "relationRules.json"
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
