import json


class DataLoader:
    """
    Loads data from JSON files into the database.
    """

    def __init__(self, db):
        """
        Initialize the data loader.

        :param db: Database instance used to execute SQL queries
        """
        self.db = db

    def load_rooms(self, path) -> None:
        """
        Load rooms data from a JSON file into the rooms table.

        :param path: Path to the rooms.json file
        """
        with open(path) as f:
            data = json.load(f)

        with self.db.cursor() as cursor:
            for value in data:
                cursor.execute(
                    "INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (value["id"], value["name"])
                )
        self.db.commit()

    def load_students(self, path) -> None:
        """
        Load students data from a JSON file into the students table.

        :param path: Path to the students.json file
        """
        with open(path) as f:
            data = json.load(f)

        with self.db.cursor() as cursor:
            for value in data:
                cursor.execute(
                    """INSERT INTO students
                       (id, name, birthday, sex, room_id)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    (value["id"], value["name"], value["birthday"][:10], value["sex"], value["room"])
                )
        self.db.commit()
