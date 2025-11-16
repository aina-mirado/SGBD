import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Any

from src.usefonctions import get_current_db


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


    @staticmethod
    def describe_table(table_name: str, db_name: Optional[str] = None, base_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        base = Path(base_path) if base_path else Path.cwd() / "Data"

        # si db_name non fourni, tenter de lire la DB courante persistée
        db_name = db_name or get_current_db()

        if not db_name:
            return {"error":"no database selected"}

        file = base / db_name / "informationTable.json"
        if not file.exists():
            return None

        try:
            data = json.loads(file.read_text(encoding="utf-8"))
        except Exception:
            return None

        return next(
            (t.copy() for t in data.get("tables", []) if t.get("name") == table_name),
            None
        )

    # helper: convert raw token to python value
    @staticmethod
    def _to_python(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, str):
            v = raw.strip()
            if v.upper() == "NULL":
                return None
            if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
                return v[1:-1]
            try:
                return int(v)
            except Exception:
                try:
                    return float(v)
                except Exception:
                    low = v.lower()
                    if low in ("true", "false"):
                        return low == "true"
                    return v
        return raw

    @staticmethod
    def check_data_type(value: Any, type_str: str) :
        """
        Vérifie / convertit `value` selon type_str supporté: INT, FLOAT, VARCHAR(n), TEXT, TIMESTAMP
        Retourne (converted_value, ok, error_msg)
        """
        if value is None:
            return None, True, None
        typ = (type_str or "").strip().upper()
        try:
            if typ.startswith("INT"):
                if isinstance(value, bool):
                    return None, False, "invalid INT"
                return int(value), True, None
            if typ.startswith("FLOAT") or typ.startswith("REAL") or typ.startswith("DOUBLE"):
                return float(value), True, None
            if typ.startswith("VARCHAR"):
                m = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", typ)
                s = str(value)
                if m and len(s) > int(m.group(1)):
                    return None, False, "VARCHAR length exceeded"
                return s, True, None
            if typ == "TEXT":
                return str(value), True, None
            if typ == "TIMESTAMP":
                # accept string / numeric — keep as string for now
                return str(value), True, None
            # fallback: accept as string
            return str(value), True, None
        except Exception:
            return None, False, "type conversion failed"

    @staticmethod
    def check_constraints(col_meta: Dict[str, Any], value: Any, existing_rows: List[Dict[str, Any]], column_name: str) :
        """
        Vérifie contraintes pour une colonne et éventuellement transforme la valeur (DEFAULT).
        Contraintes supportées: PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT, UNIQUE, DEFAULT
        Renvoie (value_maybe_changed, ok, error)
        """
        cons = [str(c).upper() for c in (col_meta.get("constraints") or [])]
        cons_str = " ".join(cons)
        # DEFAULT handling
        default_val = None
        for c in col_meta.get("constraints") or []:
            if isinstance(c, dict) and "DEFAULT" in (k.upper() for k in c.keys()):
                default_val = list(c.values())[0]
        if value is None:
            if default_val is not None:
                val_conv, ok, err = Table.check_data_type(default_val, col_meta.get("type", ""))
                if not ok:
                    return None, False, f"DEFAULT type error for {column_name}"
                value = val_conv
            elif any("AUTO_INCREMENT" in x for x in cons):
                # leave None -> caller will assign next auto value
                return None, True, None
            elif "NOT" in cons_str and "NULL" in cons_str:
                return None, False, f"NOT NULL violation on {column_name}"
            elif any("PRIMARY" in x for x in cons):
                return None, False, f"PRIMARY KEY cannot be NULL ({column_name})"
            else:
                return None, True, None

        # UNIQUE check
        if any("UNIQUE" in x for x in cons):
            for r in existing_rows:
                if r.get(column_name) == value:
                    return None, False, f"UNIQUE violation on {column_name}"

        return value, True, None

    @staticmethod
    def insert(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        parsed attendu minimalement:
          {"action":"INSERT", "table":"T" or "table_name":"T", "columns":["c1","c2"] (opt), "values":[v1, v2]}
        Retour: {"inserted":True, "row": {...}} ou {"inserted":False, "error": "..."}
        """
        base = Path(base_path) if base_path else Path.cwd() / "Data"
        table_name = parsed.get("table") or parsed.get("table_name")
        if not table_name:
            return {"inserted": False, "error": "no_table_name"}

        db_name = db_name or get_current_db()
        if not db_name:
            return {"inserted": False, "error": "no_database_selected"}

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema or isinstance(schema, dict) and schema.get("error"):
            return {"inserted": False, "error": "table_not_found"}

        cols_meta = schema.get("columns", [])
        cols_order = [c["name"] for c in cols_meta]

        values = parsed.get("values")
        if values is None:
            return {"inserted": False, "error": "no_values_provided"}

        cols = parsed.get("columns") or cols_order
        if len(cols) != len(values):
            return {"inserted": False, "error": "columns_values_mismatch"}

        data_file = base / db_name / f"{table_name}.json"
        # read existing rows
        rows = []
        if data_file.exists():
            try:
                df = json.loads(data_file.read_text(encoding="utf-8"))
                rows = df.get("rows", [])
            except Exception:
                rows = []

        # build new row with checks
        new_row: Dict[str, Any] = {}
        auto_columns: List[str] = []
        # map column meta by name for fast access
        meta_map = {c["name"]: c for c in cols_meta}

        for cname, raw in zip(cols, values):
            if cname not in meta_map:
                return {"inserted": False, "error": f"unknown column {cname}"}
            col_meta = meta_map[cname]
            raw_py = Table._to_python(raw)
            # data type check / conversion
            conv, ok, err = Table.check_data_type(raw_py, col_meta.get("type", ""))
            if not ok:
                # allow None when AUTO_INCREMENT
                if raw_py is None and any("AUTO_INCREMENT" in str(t).upper() for t in (col_meta.get("constraints") or [])):
                    conv = None
                else:
                    return {"inserted": False, "error": f"type error on {cname}: {err}"}
            # constraint checks (may assign DEFAULT or reject)
            conv2, ok2, err2 = Table.check_constraints(col_meta, conv, rows, cname)
            if not ok2:
                return {"inserted": False, "error": err2}
            new_row[cname] = conv2
            if any("AUTO_INCREMENT" in str(t).upper() for t in (col_meta.get("constraints") or [])):
                auto_columns.append(cname)

        # handle AUTO_INCREMENT assignment for columns that are still None
        for ac in auto_columns:
            if new_row.get(ac) is None:
                maxv = 0
                for r in rows:
                    try:
                        v = int(r.get(ac) or 0)
                        if v > maxv:
                            maxv = v
                    except Exception:
                        continue
                new_row[ac] = maxv + 1

        # ensure PRIMARY KEY uniqueness if multi PKs exist
        pk_cols = [c["name"] for c in cols_meta if any("PRIMARY" in str(x).upper() for x in (c.get("constraints") or []))]
        if pk_cols:
            for r in rows:
                if all(r.get(k) == new_row.get(k) for k in pk_cols):
                    return {"inserted": False, "error": "PRIMARY KEY violation"}

        # append and save
        rows.append(new_row)
        try:
            data_file.parent.mkdir(parents=True, exist_ok=True)
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump({"rows": rows}, f, indent=2, ensure_ascii=False)
        except Exception as e:
            return {"inserted": False, "error": "io_error", "detail": str(e)}

        return {"inserted": True, "row": new_row}

    @staticmethod
    def update(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        parsed attendu minimalement:
          {"action":"UPDATE", "table":"T" or "table_name":"T", "set":{...}, "where":{...} (opt)}
        Retour: {"updated":True, "row": {...}} ou {"updated":False, "error": "..."}
        """
        base = Path(base_path) if base_path else Path.cwd() / "Data"
        table_name = parsed.get("table") or parsed.get("table_name")
        if not table_name:
            return {"updated": False, "error": "no_table_name"}

        db_name = db_name or get_current_db()
        if not db_name:
            return {"updated": False, "error": "no_database_selected"}

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema or isinstance(schema, dict) and schema.get("error"):
            return {"updated": False, "error": "table_not_found"}

        cols_meta = schema.get("columns", [])
        cols_order = [c["name"] for c in cols_meta]

        set_values = parsed.get("set")
        if set_values is None:
            return {"updated": False, "error": "no_set_values_provided"}

        # read existing rows
        data_file = base / db_name / f"{table_name}.json"
        rows = []
        if data_file.exists():
            try:
                df = json.loads(data_file.read_text(encoding="utf-8"))
                rows = df.get("rows", [])
            except Exception:
                rows = []

        # build updated rows with checks
        updated_rows: List[Dict[str, Any]] = []
        auto_columns: List[str] = []
        for r in rows:
            # check WHERE conditions
            where = parsed.get("where")
            if where:
                match = True
                for wc, wv in where.items():
                    if r.get(wc) != wv:
                        match = False
                        break
                if not match:
                    updated_rows.append(r)
                    continue  # no update for this row

            new_row: Dict[str, Any] = {}
            # map column meta by name for fast access
            meta_map = {c["name"]: c for c in cols_meta}

            for cname, raw in set_values.items():
                if cname not in meta_map:
                    return {"updated": False, "error": f"unknown column {cname}"}
                col_meta = meta_map[cname]
                raw_py = Table._to_python(raw)
                # data type check / conversion
                conv, ok, err = Table.check_data_type(raw_py, col_meta.get("type", ""))
                if not ok:
                    # allow None when AUTO_INCREMENT
                    if raw_py is None and any("AUTO_INCREMENT" in str(t).upper() for t in (col_meta.get("constraints") or [])):
                        conv = None
                    else:
                        return {"updated": False, "error": f"type error on {cname}: {err}"}
                # constraint checks (may assign DEFAULT or reject)
                conv2, ok2, err2 = Table.check_constraints(col_meta, conv, rows, cname)
                if not ok2:
                    return {"updated": False, "error": err2}
                new_row[cname] = conv2
                if any("AUTO_INCREMENT" in str(t).upper() for t in (col_meta.get("constraints") or [])):
                    auto_columns.append(cname)

            updated_rows.append(new_row)

        # handle AUTO_INCREMENT assignment for columns that are still None
        for ac in auto_columns:
            for ur in updated_rows:
                if ur.get(ac) is None:
                    maxv = 0
                    for r in rows:
                        try:
                            v = int(r.get(ac) or 0)
                            if v > maxv:
                                maxv = v
                        except Exception:
                            continue
                    ur[ac] = maxv + 1

        # ensure PRIMARY KEY uniqueness if multi PKs exist
        pk_cols = [c["name"] for c in cols_meta if any("PRIMARY" in str(x).upper() for x in (c.get("constraints") or []))]
        if pk_cols:
            for ur in updated_rows:
                for r in rows:
                    if all(r.get(k) == ur.get(k) for k in pk_cols):
                        return {"updated": False, "error": "PRIMARY KEY violation"}

        # save updated rows
        try:
            data_file.parent.mkdir(parents=True, exist_ok=True)
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump({"rows": updated_rows}, f, indent=2, ensure_ascii=False)
        except Exception as e:
            return {"updated": False, "error": "io_error", "detail": str(e)}

        return {"updated": True, "row": updated_rows}

    @staticmethod
    def delete(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        parsed attendu minimalement:
          {"action":"DELETE", "table":"T" or "table_name":"T", "where":{...} (opt)}
        Retour: {"deleted":True, "row": {...}} ou {"deleted":False, "error": "..."}
        """
        base = Path(base_path) if base_path else Path.cwd() / "Data"
        table_name = parsed.get("table") or parsed.get("table_name")
        if not table_name:
            return {"deleted": False, "error": "no_table_name"}

        db_name = db_name or get_current_db()
        if not db_name:
            return {"deleted": False, "error": "no_database_selected"}

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema or isinstance(schema, dict) and schema.get("error"):
            return {"deleted": False, "error": "table_not_found"}

        # read existing rows
        data_file = base / db_name / f"{table_name}.json"
        rows = []
        if data_file.exists():
            try:
                df = json.loads(data_file.read_text(encoding="utf-8"))
                rows = df.get("rows", [])
            except Exception:
                rows = []

        # filter out rows to delete
        where = parsed.get("where")
        if where:
            rows = [r for r in rows if not all(r.get(k) == v for k, v in where.items())]

        # save updated rows
        try:
            data_file.parent.mkdir(parents=True, exist_ok=True)
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump({"rows": rows}, f, indent=2, ensure_ascii=False)
        except Exception as e:
            return {"deleted": False, "error": "io_error", "detail": str(e)}

        return {"deleted": True}

