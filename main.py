import sys
from pathlib import Path
from xml.etree.ElementTree import ParseError

from src.usefonctions import showResult

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.cli import cli
from src.parser import parser
from src.executor import executor


def main():
    print("Bienvenue dans le mini SGBD CLI (tape 'HELP' pour la liste des commandes)")
    for query in cli():
        parsed = parser(query)
        result = executor(parsed)
        showResult(result)

if __name__ == "__main__":
    main()