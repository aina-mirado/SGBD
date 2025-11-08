from xml.etree.ElementTree import ParseError
from src.cli import cli
from src.parser import parser

def main():
    print("Bienvenue dans le mini SGBD CLI (tape 'HELP' pour la liste des commandes)")
    for query in cli():
        print(parser(query))


if __name__ == "__main__":
    main()