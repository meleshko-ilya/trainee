from enum import Enum

class Queries(Enum):
    ROOMS_COUNT = """
    SELECT rooms.id, rooms.name, COUNT(students.id) AS students_count
    FROM rooms
    LEFT JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name;
    """

    LOWEST_AVG_AGE = """
    SELECT rooms.id, rooms.name,
           AVG(EXTRACT(YEAR FROM AGE(students.birthday))) AS avg_age
    FROM rooms
    JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name
    ORDER BY avg_age
    LIMIT 5;
    """

    MAX_AGE_DIFF = """
    SELECT rooms.id, rooms.name,
           MAX(EXTRACT(YEAR FROM AGE(students.birthday)))
         - MIN(EXTRACT(YEAR FROM AGE(students.birthday))) AS age_diff
    FROM rooms
    JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name
    ORDER BY age_diff DESC
    LIMIT 5;
    """

    MIXED_SEX = """
    SELECT rooms.id, rooms.name
    FROM rooms
    JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name
    HAVING COUNT(DISTINCT students.sex) > 1;
    """
