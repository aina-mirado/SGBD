import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
import csv
from src.usefonctions import get_current_db
import pandas as pd



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

    @staticmethod
    def _to_python(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, str):
            v = raw.strip()
            if v == "":
                return None
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
    def _check_data_type(value: Any, type_str: str) :
        """
        Validate/convert value according to supported types:
        INT, FLOAT, VARCHAR(n), TEXT, TIMESTAMP
        """
        if value is None:
            return None, True, None
        t = (type_str or "").strip().upper()
        try:
            if t.startswith("INT"):
                if isinstance(value, bool):
                    return None, False, "invalid INT"
                return int(value), True, None
            if t.startswith("FLOAT") or t in ("REAL", "DOUBLE"):
                return float(value), True, None
            if t.startswith("VARCHAR"):
                m = re.search(r"VARCHAR\s*\(\s*(\d+)\s*\)", t)
                s = str(value)
                if m and len(s) > int(m.group(1)):
                    return None, False, "VARCHAR length exceeded"
                return s, True, None
            if t == "TEXT":
                return str(value), True, None
            if t == "TIMESTAMP":
                # accept string or numeric, keep as ISO-like string
                return str(value), True, None
            return str(value), True, None
        except Exception:
            return None, False, "type conversion failed"

    @staticmethod
    def _check_constraints(col_meta: Dict[str, Any], value: Any, rows: List[Dict[str, Any]], column_name: str) :
        """
        Apply constraints: PRIMARY_KEY, NOT_NULL, AUTO_INCREMENT, UNIQUE, DEFAULT, CURRENT_TIMESTAMP.
        Returns (value_maybe_modified, ok, error_msg)
        """
        cons = col_meta.get("constraints") or []
        # normalize constraints tokens to strings/dicts
        cons_u = [str(x).upper() if not isinstance(x, dict) else x for x in cons]

        # DEFAULT handling (support {"DEFAULT": val} or DEFAULT token as next token handled at parse)
        default_val = None
        for c in cons:
            if isinstance(c, dict) and any(k.upper() == "DEFAULT" for k in c.keys()):
                # get first value
                default_val = list(c.values())[0]

        # CURRENT_TIMESTAMP support for TIMESTAMP when provided as token (not as DEFAULT value)
        if (value is None) and default_val is None and any(str(x).upper() == "CURRENT_TIMESTAMP" for x in cons_u):
            return datetime.utcnow().isoformat(sep=' '), True, None

        if value is None and default_val is not None:
            # special-case DEFAULT CURRENT_TIMESTAMP (or similar)
            try:
                if isinstance(default_val, str) and default_val.strip().upper().startswith("CURRENT_TIMESTAMP"):
                    value = datetime.utcnow().isoformat(sep=' ')
                else:
                    dv = default_val
                    conv, ok, err = Table._check_data_type(Table._to_python(dv), col_meta.get("type", ""))
                    if not ok:
                        return None, False, f"DEFAULT type error for {column_name}"
                    value = conv
            except Exception:
                return None, False, f"DEFAULT handling error for {column_name}"

        # NOT NULL / PRIMARY_KEY cannot be null
        if value is None:
            if any(str(x).upper() == "AUTO_INCREMENT" for x in cons_u):
                # let caller assign auto-increment later
                return None, True, None
            if any(str(x).upper() == "NOT_NULL" or str(x).upper() == "NOT NULL" for x in cons_u):
                return None, False, f"NOT NULL violation on {column_name}"
            if any(str(x).upper().startswith("PRIMARY") for x in cons_u):
                return None, False, f"PRIMARY KEY cannot be NULL ({column_name})"
            return None, True, None

        # UNIQUE check
        if any(str(x).upper() == "UNIQUE" for x in cons_u):
            for r in rows:
                if r.get(column_name) == value:
                    return None, False, f"UNIQUE violation on {column_name}"

        return value, True, None

    @staticmethod
    def insert_into_table(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """l'affichage de donner  dans le <table_name>.csv que ce soit suivant sont colomn ou  ou toute et avec condition ou pas.
        Insert parsed values into existing <table>.csv after validating types & constraints.
        parsed example:
          {"action":"INSERT","table_name":"T","columns":[...], "values":[...]}
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
        header = [c["name"] for c in cols_meta]

        values = parsed.get("values")
        if values is None:
            return {"inserted": False, "error": "no_values_provided"}

        cols = parsed.get("columns") or header
        if len(cols) != len(values):
            return {"inserted": False, "error": "columns_values_mismatch"}

        csv_file = base / db_name / f"{table_name}.csv"
        if not csv_file.exists():
            return {"inserted": False, "error": "table_data_file_not_found"}

        # read existing rows
        rows: List[Dict[str, Any]] = []
        with open(csv_file, "r", encoding="utf-8", newline='') as f:
            reader = csv.DictReader(f)
            for r in reader:
                # convert values to python types according to schema for checks
                conv = {}
                for col in header:
                    val = r.get(col)
                    conv[col] = Table._to_python(val)
                rows.append(conv)

        # build new row with validation
        new_row: Dict[str, Any] = {col: None for col in header}
        auto_cols: List[str] = []
        meta_map = {c["name"]: c for c in cols_meta}

        # fill provided columns
        for cname, raw in zip(cols, values):
            if cname not in meta_map:
                return {"inserted": False, "error": f"unknown_column:{cname}"}
            col_meta = meta_map[cname]
            raw_py = Table._to_python(raw)
            conv, ok, err = Table._check_data_type(raw_py, col_meta.get("type", ""))
            if not ok:
                # allow None for auto_increment
                if raw_py is None and any(str(x).upper() == "AUTO_INCREMENT" for x in (col_meta.get("constraints") or [])):
                    conv = None
                else:
                    return {"inserted": False, "error": f"type error on {cname}: {err}"}
            val_after_cons, ok2, err2 = Table._check_constraints(col_meta, conv, rows, cname)
            if not ok2:
                return {"inserted": False, "error": err2}
            new_row[cname] = val_after_cons
            if any(str(x).upper() == "AUTO_INCREMENT" for x in (col_meta.get("constraints") or [])):
                auto_cols.append(cname)

        # for columns not provided, try defaults / CURRENT_TIMESTAMP / AUTO_INCREMENT
        for col in header:
            if col in cols:
                continue
            meta = meta_map.get(col)
            if not meta:
                continue
            # apply DEFAULT/CURRENT_TIMESTAMP/NOT NULL checks for missing columns
            val_after_cons, ok2, err2 = Table._check_constraints(meta, None, rows, col)
            if not ok2:
                return {"inserted": False, "error": err2}
            new_row[col] = val_after_cons
            if any(str(x).upper() == "AUTO_INCREMENT" for x in (meta.get("constraints") or [])):
                auto_cols.append(col)

        # assign AUTO_INCREMENT values
        for ac in auto_cols:
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

        # PRIMARY KEY uniqueness: check columns with PRIMARY constraint
        pk_cols = [c["name"] for c in cols_meta if any(str(x).upper().startswith("PRIMARY") for x in (c.get("constraints") or []))]
        if pk_cols:
            for r in rows:
                if all(r.get(k) == new_row.get(k) for k in pk_cols):
                    return {"inserted": False, "error": "PRIMARY KEY violation"}

        # final type normalization (ensure types saved as strings in CSV)
        out_row = {h: ("" if new_row.get(h) is None else str(new_row.get(h))) for h in header}

        # append to CSV
        try:
            with open(csv_file, "a", encoding="utf-8", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writerow(out_row)
        except Exception as e:
            return {"inserted": False, "error": "io_error", "detail": str(e)}

        return {"inserted": True, "row": new_row}

    @staticmethod
    def select(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Exécute un SELECT simple (single-table) basé sur la sortie du parser.
        Supporte: DISTINCT, columns list, WHERE (simple expressions),
        ORDER BY, GROUP BY (basique), HAVING (basique), LIMIT (offset,count or count).
        Retourne dict: {"action":"SELECT","rows":[{...},...],"columns":[...],"count":n}
        """

        base = Path(base_path) if base_path else Path.cwd() / "Data"
        db_name = db_name or get_current_db()
        if not db_name:
            return {"error": "no_database_selected"}

        # from peut être liste ou string (joins). On gère seulement single table (premier)
        frm = parsed.get("from")
        if not frm:
            return {"error": "no_from"}
        table_name = None
        if isinstance(frm, list):
            if len(frm) == 0:
                return {"error": "no_from_table"}
            table_name = frm[0]
        else:
            # string: detect simple table name or refuse complex JOINs
            if isinstance(frm, str) and re.search(r"\s+JOIN\s+", frm, re.IGNORECASE):
                return {"error": "joins_not_supported"}
            table_name = str(frm).split()[0]

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema:
            return {"error": "table_not_found", "table": table_name}

        cols_meta = schema.get("columns", [])
        header = [c["name"] for c in cols_meta]
        csv_file = base / db_name / f"{table_name}.csv"
        if not csv_file.exists():
            return {"error": "table_data_file_not_found", "table": table_name}

        # build dtype / parse_dates according to schema
        dtype_map = {}
        parse_dates = []
        for c in cols_meta:
            t = (c.get("type") or "").upper()
            if t.startswith("INT"):
                dtype_map[c["name"]] = "Int64"
            elif t.startswith("FLOAT") or t in ("REAL", "DOUBLE"):
                dtype_map[c["name"]] = "float"
            elif t == "TIMESTAMP":
                parse_dates.append(c["name"])
            else:
                # VARCHAR/TEXT -> string
                dtype_map[c["name"]] = "string"

        # read csv with pandas
        try:
            # lire sans parse_dates d'abord
            df = pd.read_csv(csv_file, dtype=dtype_map, keep_default_na=False)
            
            # convertir manuellement les colonnes TIMESTAMP avec format ISO strict
            for col in parse_dates:
                if col in df.columns:
                    # utiliser format ISO explicite (avec espace entre date et heure)
                    df[col] = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S")
        except Exception:
            # fallback to generic read
            df = pd.read_csv(csv_file, keep_default_na=False)

        # helper: translate simple SQL WHERE/HAVING to pandas query
        def sql_to_pandas_expr(s: str) -> str:
            if not s:
                return ""
            expr = s
            # replace <> with !=
            expr = re.sub(r"<>", "!=", expr)
            # replace = (but avoid changing >= <= != ==) -> replace standalone =
            expr = re.sub(r"(?<=[^\!<>=])=(?=[^=])", "==", expr)
            # AND/OR/NOT -> and/or/not for pandas query (use bitwise ops)
            expr = re.sub(r"\bAND\b", " and ", expr, flags=re.IGNORECASE)
            expr = re.sub(r"\bOR\b", " or ", expr, flags=re.IGNORECASE)
            expr = re.sub(r"\bNOT\b", " not ", expr, flags=re.IGNORECASE)
            # convert SQL true/false
            expr = re.sub(r"\bTRUE\b", "True", expr, flags=re.IGNORECASE)
            expr = re.sub(r"\bFALSE\b", "False", expr, flags=re.IGNORECASE)
            return expr

        # apply WHERE
        where = parsed.get("where")
        if where:
            try:
                q = sql_to_pandas_expr(where)
                df = df.query(q)
            except Exception as e:
                return {"error": "where_evaluation_failed", "detail": str(e), "where": where}

        # selection of columns
        sel_cols = parsed.get("columns") or ["*"]
        if sel_cols == ["*"]:
            out_df = df.copy()
        else:
            # keep only existing columns, preserve order
            cols_keep = [c for c in sel_cols if c in df.columns]
            out_df = df[cols_keep]

        # GROUP BY basic: take first row per group (no aggregate parsing)
        group_by = parsed.get("group_by") or []
        if group_by:
            try:
                out_df = out_df.groupby(group_by, dropna=False, as_index=False).first()
            except Exception as e:
                return {"error": "group_by_failed", "detail": str(e)}

            having = parsed.get("having")
            if having:
                try:
                    qh = sql_to_pandas_expr(having)
                    out_df = out_df.query(qh)
                except Exception as e:
                    return {"error": "having_evaluation_failed", "detail": str(e), "having": having}

        # DISTINCT
        if parsed.get("distinct"):
            out_df = out_df.drop_duplicates()

        # ORDER BY
        order_by = parsed.get("order_by") or []
        if order_by:
            cols_ob = [o["column"] for o in order_by if o.get("column") in out_df.columns]
            asc_list = [True if o.get("dir", "ASC").upper() == "ASC" else False for o in order_by if o.get("column") in out_df.columns]
            if cols_ob:
                out_df = out_df.sort_values(by=cols_ob, ascending=asc_list or True, kind="mergesort")

        # LIMIT
        limit = parsed.get("limit")
        if limit:
            if isinstance(limit, dict):
                if "offset" in limit and "count" in limit:
                    o, c = int(limit["offset"]), int(limit["count"])
                    out_df = out_df.iloc[o:o+c]
                elif "count" in limit:
                    c = int(limit["count"])
                    out_df = out_df.iloc[:c]
            else:
                try:
                    n = int(limit)
                    out_df = out_df.iloc[:n]
                except Exception:
                    pass

        # prepare rows as list of dicts (convert Timestamp to iso str)
        def row_to_serializable(r):
            res = {}
            for k, v in r.items():
                if pd.isna(v):
                    res[k] = None
                elif hasattr(v, "isoformat"):  # Timestamp
                    res[k] = v.isoformat(sep=' ')
                else:
                    # convert numpy types to python native
                    try:
                        res[k] = v.item()
                    except Exception:
                        res[k] = v
                # ensure strings are regular str
                if isinstance(res[k], (str,)):
                    res[k] = str(res[k])
            return res

        rows = [row_to_serializable(r) for _, r in out_df.iterrows()]
        return {
            "action": "SELECT",
            "table": table_name,
            "columns": list(out_df.columns),
            "count": len(rows),
            "rows": rows
        }

    @staticmethod
    def delete_in_table(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Supprime des lignes d'une table selon une condition WHERE optionnelle.
        parsed attendu:
          {"action":"DELETE","table_name":"T","condition":"col1=val1 AND col2=val2"} (optionnel)
        Si condition est None/absent, supprime TOUTES les lignes.
        Retourne: {"deleted":True, "count": n_deleted} ou {"deleted":False, "error": "..."}
        """
        import pandas as pd
        import re

        base = Path(base_path) if base_path else Path.cwd() / "Data"
        table_name = parsed.get("table") or parsed.get("table_name")
        if not table_name:
            return {"deleted": False, "error": "no_table_name"}

        db_name = db_name or get_current_db()
        if not db_name:
            return {"deleted": False, "error": "no_database_selected"}

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema:
            return {"deleted": False, "error": "table_not_found"}

        csv_file = base / db_name / f"{table_name}.csv"
        if not csv_file.exists():
            return {"deleted": False, "error": "table_data_file_not_found"}

        # read csv
        try:
            df = pd.read_csv(csv_file, keep_default_na=False)
        except Exception as e:
            return {"deleted": False, "error": "io_error", "detail": str(e)}

        initial_count = len(df)

        # apply WHERE condition if provided
        condition = parsed.get("condition")
        if condition:
            try:
                # convert SQL syntax to pandas query syntax
                q = condition
                q = re.sub(r"<>", "!=", q)
                q = re.sub(r"(?<=[^\!<>=])=(?=[^=])", "==", q)
                q = re.sub(r"\bAND\b", " and ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bOR\b", " or ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bNOT\b", " not ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bTRUE\b", "True", q, flags=re.IGNORECASE)
                q = re.sub(r"\bFALSE\b", "False", q, flags=re.IGNORECASE)
                
                # keep rows that DON'T match the condition (inverse of query)
                mask = df.eval(q)
                df = df[~mask]
            except Exception as e:
                return {"deleted": False, "error": "condition_evaluation_failed", "detail": str(e)}
        else:
            # no condition: delete all rows
            df = df.iloc[0:0]  # empty dataframe keeping columns

        deleted_count = initial_count - len(df)

        # write back to csv
        try:
            import csv
            with open(csv_file, "w", encoding="utf-8", newline='') as f:
                df.to_csv(f, index=False, quoting=csv.QUOTE_MINIMAL)
        except Exception as e:
            return {"deleted": False, "error": "io_error", "detail": str(e)}

        return {"deleted": True, "count": deleted_count, "table": table_name}

# ...existing code...
    @staticmethod
    def update_value_table(parsed: Dict[str, Any], db_name: Optional[str] = None, base_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Met à jour des lignes dans une table selon une condition WHERE optionnelle.
        parsed attendu:
          {"action":"UPDATE","table_name":"T","assignments":{"col1":"val1","col2":"val2"},"condition":"id=1"}
        Retourne: {"updated":True, "count": n_updated} ou {"updated":False, "error": "..."}
        """


        base = Path(base_path) if base_path else Path.cwd() / "Data"
        table_name = parsed.get("table") or parsed.get("table_name")
        if not table_name:
            return {"updated": False, "error": "no_table_name"}

        db_name = db_name or get_current_db()
        if not db_name:
            return {"updated": False, "error": "no_database_selected"}

        schema = Table.describe_table(table_name, db_name=db_name, base_path=base_path)
        if not schema:
            return {"updated": False, "error": "table_not_found"}

        cols_meta = schema.get("columns", [])
        header = [c["name"] for c in cols_meta]
        meta_map = {c["name"]: c for c in cols_meta}

        csv_file = base / db_name / f"{table_name}.csv"
        if not csv_file.exists():
            return {"updated": False, "error": "table_data_file_not_found"}

        # read csv
        try:
            df = pd.read_csv(csv_file, keep_default_na=False)
        except Exception as e:
            return {"updated": False, "error": "io_error", "detail": str(e)}

        # apply WHERE condition to filter rows
        condition = parsed.get("condition")
        mask_update = None
        if condition:
            try:
                q = condition
                q = re.sub(r"<>", "!=", q)
                q = re.sub(r"(?<=[^\!<>=])=(?=[^=])", "==", q)
                q = re.sub(r"\bAND\b", " and ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bOR\b", " or ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bNOT\b", " not ", q, flags=re.IGNORECASE)
                q = re.sub(r"\bTRUE\b", "True", q, flags=re.IGNORECASE)
                q = re.sub(r"\bFALSE\b", "False", q, flags=re.IGNORECASE)
                mask_update = df.eval(q)
            except Exception as e:
                return {"updated": False, "error": "condition_evaluation_failed", "detail": str(e)}
        else:
            # no condition: update all rows
            mask_update = pd.Series([True] * len(df))

        # validate and apply assignments
        assignments = parsed.get("assignments") or {}
        if not assignments:
            return {"updated": False, "error": "no_assignments"}

        for col_name, raw_val in assignments.items():
            if col_name not in meta_map:
                return {"updated": False, "error": f"unknown_column:{col_name}"}

            col_meta = meta_map[col_name]
            raw_py = Table._to_python(raw_val)
            
            # type check / conversion
            conv, ok, err = Table._check_data_type(raw_py, col_meta.get("type", ""))
            if not ok:
                return {"updated": False, "error": f"type error on {col_name}: {err}"}

            # constraint checks (NOT NULL, UNIQUE, etc.)
            rows_list = df[mask_update].to_dict('records')
            val_final, ok2, err2 = Table._check_constraints(col_meta, conv, rows_list, col_name)
            if not ok2:
                return {"updated": False, "error": err2}

            # update the column where mask_update is True
            df.loc[mask_update, col_name] = val_final

        updated_count = mask_update.sum()

        # write back to csv
        try:
            with open(csv_file, "w", encoding="utf-8", newline='') as f:
                df.to_csv(f, index=False, quoting=csv.QUOTE_MINIMAL)
        except Exception as e:
            return {"updated": False, "error": "io_error", "detail": str(e)}

        return {"updated": True, "count": int(updated_count), "table": table_name}
