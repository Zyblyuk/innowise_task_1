CREATE TABLE rooms
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL UNIQUE
);


CREATE TABLE students
(
    id       SERIAL PRIMARY KEY,
    room INTEGER NOT NULL,
    name     VARCHAR(50) NOT NULL,
    sex      VARCHAR(1)  NOT NULL,
    birthday timestamp    NOT NULL
);


ALTER TABLE students ADD CONSTRAINT fk_room FOREIGN KEY (room) REFERENCES rooms (id) ON DELETE CASCADE;