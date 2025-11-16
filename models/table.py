from typing import List, Dict, Optional, Any


class Table:
    _name: str
    _columns: List[Dict[str, Any]]
    _primary_keys: List[str]
    _foreign_keys: List[Dict[str, Any]]

    def __init__(
        self,
        name: str,
        columns: Optional[List[Dict[str, Any]]] = None,
        primary_keys: Optional[List[str]] = None,
        foreign_keys: Optional[List[Dict[str, Any]]] = None,
    ):
        self._name = name
        self._columns = columns[:] if columns else []
        self._primary_keys = primary_keys[:] if primary_keys else []
        # chaque élément: {"column_name": "...", "referenced_table":"...", "referenced_column":"...", "on_delete":..., "on_update":...}
        self._foreign_keys = [fk.copy() for fk in (foreign_keys or [])]

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> List[Dict[str, Any]]:
        return [c.copy() for c in self._columns]

    @property
    def primary_keys(self) -> List[str]:
        return list(self._primary_keys)

    @property
    def foreign_keys(self) -> List[Dict[str, Any]]:
        return [fk.copy() for fk in self._foreign_keys]

    