import json

def showResult(result):
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

    if "dropped" in result:
        if result.get("dropped"):
            print("Supprimé:", result.get("database") or result.get("table"))
        else:
            if result.get("skipped"):
                print("Suppression ignorée (if_exists).")
            else:
                print("Suppression échouée:", result.get("error", ""))
        return

    print("RESULT:", json.dumps(result, ensure_ascii=False, indent=2))