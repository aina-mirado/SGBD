import sys
from pathlib import Path

# ajoute la racine du projet au PYTHONPATH (permet d'importer models depuis src)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.databases import Database

def main(parsed):

    if 
    db = Database("nomDB")
    created = db.create_db(exist_ok=False)
    print("create_db ->", created, "path:", db.path)

    # renommer la base
    renamed = db.modify_db("nomDB_renamed")
    print("modify_db ->", renamed, "new path:", db.path)

    # supprimer la base
    removed = db.remove_db()
    print("remove_db ->", removed)

if __name__ == "__main__":
    main()