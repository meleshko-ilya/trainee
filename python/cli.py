import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Students/Rooms DB tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_load = subparsers.add_parser("load", help="Load students and rooms data into DB")
    parser_load.add_argument("--students", required=True, help="Path to students JSON file")
    parser_load.add_argument("--rooms", required=True, help="Path to rooms JSON file")

    parser_export = subparsers.add_parser("export", help="Export data from DB")
    parser_export.add_argument("--format", choices=["json", "xml"], required=True, help="Output format")

    return parser.parse_args()