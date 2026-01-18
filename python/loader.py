import json

class DataLoader:
    def __init__(self, db):
        self.db = db

    def load_rooms(self, path) -> None:
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
