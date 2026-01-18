-- Database name - trainee

CREATE TABLE rooms (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE students (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    birthday DATE NOT NULL,
    sex CHAR(1) NOT NULL,
    room_id INT NOT NULL REFERENCES rooms(id)
);
