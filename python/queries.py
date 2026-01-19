from enum import Enum


class Queries(Enum):
    """
    Collection of SQL queries used by the application.

    Each enum value represents a single SQL query that is executed
    directly on the database.
    """

    ROOMS_COUNT = """
    SELECT rooms.id, rooms.name, COUNT(students.id) AS students_count
    FROM rooms
    LEFT JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name;
    """
    """
    Returns a list of rooms with the number of students living in each room.
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
    """
    Returns 5 rooms with the lowest average age of students.
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
    """
    Returns 5 rooms with the largest age difference between students.
    """

    MIXED_SEX = """
    SELECT rooms.id, rooms.name
    FROM rooms
    JOIN students ON students.room_id = rooms.id
    GROUP BY rooms.id, rooms.name
    HAVING COUNT(DISTINCT students.sex) > 1;
    """
    """
    Returns rooms where students of different sexes live together.
    """
