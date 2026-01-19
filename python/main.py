from db import Database
from loader import DataLoader
from queries import Queries
from exporter import Exporter
from cli import parse_args
import os


def run_query(db, sql) -> (list, list):
    """
    Execute an SQL query on the database.

    :param db: Database instance
    :param sql: SQL query string
    :return: Tuple (rows, cols) where:
             - rows is a list of result rows
             - cols is a list of column names
    """
    with db.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    return rows, cols

def main() -> None:
    """
    Application entry point.

    Depending on the CLI command:
    - load   : loads data from JSON files into the database
    - export : executes SQL queries and saves results
               in JSON or XML format into the output/ directory
   """
    args = parse_args()
    db = Database()

    if args.command == "load":
        loader = DataLoader(db)
        loader.load_rooms(args.rooms)
        loader.load_students(args.students)

    elif args.command == "export":
        os.makedirs("output", exist_ok=True)

        for query in Queries:
            rows, cols = run_query(db, query.value)
            file_path = os.path.join("output", f"{query.name}.{args.format}")

            if args.format == "json":
                Exporter.json(rows, cols, file_path)
            elif args.format == "xml":
                Exporter.xml(rows, cols, file_path)

if __name__ == "__main__":
    main()
