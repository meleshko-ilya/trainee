from db import Database
from loader import DataLoader
from queries import Queries
from exporter import Exporter
from cli import parse_args
import os


def run_query(db, sql) -> (list, list):
    with db.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    return rows, cols

def main() -> None:
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
