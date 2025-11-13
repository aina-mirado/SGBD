from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style # type: ignore
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.layout.processors import Processor, Transformation

# --- STYLE COMPATIBLE ---
style = Style.from_dict({
    "prompt": "bold #00ffff",   
    "arrow": "#ffff00",      
    "ghost": "italic #888888"  
})

# --- COMMANDES ---
commands = ["CREATE", "SELECT", "INSERT", "UPDATE", "DELETE", "SHOW", "EXIT", "HELP", "DROP", "USE"]
key_words = ["TABLE", "DATABASE", "SET", "VALUE"]
commands.extend(key_words)
completer = WordCompleter(commands, ignore_case=True, sentence=True)

# --- SUGGESTIONS FANTÔMES ---
SYNTAX_HINTS = {
    "CREATE D" : "CREATE DATABASE nom_du_database",
    "CREATE T": "CREATE TABLE nom_table (id INT PRIMARY KEY, nom TEXT, ...);",
    "SELECT": "SELECT * FROM nom_table WHERE condition;",
    "INSERT": "INSERT INTO nom_table VALUES (...);",
    "UPDATE": "UPDATE nom_table SET colonne=valeur WHERE condition;",
    "DELETE": "DELETE FROM nom_table WHERE condition;",
    "SHOW T": "SHOW TABLES;",
    "SHOW D": "SHOW DATABASES",
    "USE" : "DATABASE",
}


class GhostTextProcessor(Processor):
    """Affiche une suggestion (ghost text) selon la commande tapée."""
    def apply_transformation(self, transformation_input):
        text = transformation_input.document.text_before_cursor.upper().strip()
        fragments = transformation_input.fragments

        if not text:
            # Aucun texte saisi → on ne modifie rien
            return Transformation(fragments)

        for key, hint in SYNTAX_HINTS.items():
            if text == key or text.startswith(key + " "):
                # Reste du texte fantôme après la commande
                ghost = hint[len(text):]
                fragments = fragments + [("class:ghost", ghost)]
                return Transformation(fragments)

        # Par défaut : aucun texte fantôme
        return Transformation(fragments)


def cli():
    session = PromptSession()

    while True:
        try:
            user_input = session.prompt(
                HTML("<prompt>MYPROMPT</prompt><arrow> ➜ </arrow> "),
                completer=completer,
                complete_while_typing=True,
                complete_style=CompleteStyle.MULTI_COLUMN,
                style=style,
                input_processors=[GhostTextProcessor()],
            ).strip()

            if not user_input:
                continue

            cmd = user_input.upper().split()[0]

            if cmd in ["EXIT", "QUIT"]:
                print("Fermeture du CLI...")
                break
            elif cmd == "HELP":
                print("Commandes disponibles :")
                for c in commands:
                    print("  -", c)
            elif cmd in commands:
                yield user_input
            else:
                print(f"Commande inconnue : {user_input}")
                

        except KeyboardInterrupt:
            continue
        except EOFError:
            print("\nFermeture du CLI...")
            break

