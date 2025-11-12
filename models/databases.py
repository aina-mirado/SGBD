import json
import shutil
from pathlib import Path
from typing import Optional, List


class Database:
    # déclaration des attributs (privés)
    _name: str
    _base_path: Path
    _path: Path
    _tables: List[str]
    _rules_file: Path

    def __init__(self, name: str, base_path: Optional[str] = None):
        # attribution des valeurs
        self._name = name
        self._base_path = Path(base_path) if base_path else Path.cwd() / "Data"
        self._path = (self._base_path / self._name).resolve()
        self._tables = []
        self._rules_file = self._path / "relationRules.json"

    # propriétés en lecture (accès contrôlé aux attributs privés)
    @property
    def name(self) -> str:
        return self._name

    @property
    def base_path(self) -> Path:
        return self._base_path

    @property
    def path(self) -> Path:
        return self._path

    @property
    def rules_file(self) -> Path:
        return self._rules_file

    @property
    def tables(self) -> List[str]:
        return list(self._tables)

    def create_db(self, exist_ok: bool = False) -> bool:
        """
        Crée le dossier Data/<name> et un fichier relationRules.json initial.
        Si exist_ok False et le dossier existe déjà, retourne False.
        """
        try:
            self._base_path.mkdir(parents=True, exist_ok=True)
            if self._path.exists():
                return exist_ok
            self._path.mkdir(parents=True, exist_ok=False)
            initial = {"relations": []}
            with open(self._rules_file, "w", encoding="utf-8") as f:
                json.dump(initial, f, indent=2, ensure_ascii=False)

            if self._path.exists() and self._rules_file.exists():
                return True
            return False
        except Exception:
            return False

    def remove_db(self) -> bool:
        """Supprime le dossier de la base et tout son contenu."""
        try:
            if self._path.exists():
                shutil.rmtree(self._path)
            return True
        except Exception:
            return False

    def modify_db(self, new_name: str) -> bool:
        """Renomme la base (déplace le dossier) et met à jour les attributs privés."""
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
