import json
import psycopg2
from flask import Flask, jsonify
import argparse
import dicttoxml
from decouple import config


parser = argparse.ArgumentParser()

parser.add_argument("-students", default='files/students.json', type=str, help='Input dir for students.files')
parser.add_argument("-rooms", default='files/rooms.json', type=str, help='Input dir for rooms.files')
parser.add_argument("-format", type=str, help='Input format output file files or xml', required=True)

args = parser.parse_args()
args = vars(args)

app = Flask(__name__)


def db_connect() -> psycopg2.extensions.connection:
    """Database connection using psycopg2 and values from .env file"""

    with psycopg2.connect(database=config('DATABASE'),
                          user=config('BD_USER'),
                          password=config('BD_PASSWORD'),
                          host=config('HOST'),
                          port=config('PORT')) as conn:
        print(type(conn))
        return conn


def execute_sql_scripts(query_sql) -> list:
    """Execute sql scripts"""

    conn = db_connect()
    with conn.cursor() as cur:
        cur.execute(query_sql)
        return cur.fetchall()


def upload_json(json_file, table_name) -> bool:
    """Add data to database from json file"""

    query_sql = f"""insert into {table_name} 
                    select * from json_populate_recordset(NULL::{table_name}, %s) """
    with db_connect() as conn:
        with conn.cursor() as cur:
            with open(json_file) as json_file:
                data = json.load(json_file)
                try:
                    cur.execute(query_sql, (json.dumps(data),))
                except psycopg2.errors.UniqueViolation as e:
                    print('Json has been read later\n', e)
    return True


def clear_table(table_name) -> bool:
    """Clear table"""

    query_sql = f""" DELETE FROM {table_name}"""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query_sql)
    return True


def write_to_file(file_name, data) -> bool:
    """Writing data to a file"""

    if args['format'] == "json":
        with open(config('FILE_DIR') + file_name + ".json", 'w') as f:
            json.dump(data, f)
    if args['format'] == "xml":
        xml = dicttoxml.dicttoxml(data)
        with open(config('FILE_DIR') + file_name + ".xml", "wb") as f:
            f.write(xml)
    return True


@app.route('/')
def home() -> str:
    """Home page"""

    return (
        '<a class="button" href="/wjson">Write to db from files file</a><br>'
        '<a class="button" href="/clear">Clear all table</a><br><br>'
        
        '<a class="button" href="/room_list">SELECT 1</a>'
        '<br>List of rooms and the number of students in each of them<br><br>'
        
        '<a class="button" href="/small_ave">SELECT 2</a>'
        '<br>Top 5 rooms with the smallest average age of students<br><br>'
        
        '<a class="button" href="/biggest_diff">SELECT 3</a>'
        '<br>Top 5 rooms with the biggest difference in student age<br><br>'
        
        '<a class="button" href="/diff_sex">SELECT 4</a>'
        '<br>List of rooms where students of different sexes live.<br><br>'
        
        
        '<a class="button" href="/index">Create index</a><br>'
        '<a class="button" href="/delete_index">Delete indexes</a><br>'
    )


@app.route('/wjson')
def wjson() -> str:
    """Add data to database from json file"""

    upload_json(args['rooms'], 'rooms')
    upload_json(args['students'], 'students')
    return ("Done!!!")


@app.route('/clear')
def clear() -> str:
    """Clear table"""

    clear_table('rooms')
    clear_table('students')
    return ("Done!!!")


@app.route('/room_list')
def room_list() -> json:
    """List of rooms and the number of students in each of them"""

    query_sql = """ SELECT json_build_object(t1.name, COUNT(t2.room))
                    FROM rooms AS t1
                    INNER JOIN students as t2 ON t1.id = t2.room 
                    GROUP BY t1.name, t2.room
                    ORDER BY room;"""

    res = execute_sql_scripts(query_sql)

    write_to_file("select_room_list", res)

    return jsonify(res)


@app.route('/small_ave')
def small_ave() -> json:
    """Top 5 rooms with the smallest average age of students"""

    query_sql = """ SELECT json_build_object(t1.name, SUM(age(t2.birthday))/COUNT(t2.birthday)) FROM rooms AS t1
                    INNER JOIN students as t2 ON t1.id = t2.room 
                    GROUP BY t1.id
                    ORDER BY SUM(age(t2.birthday))/COUNT(t2.birthday) LIMIT 5;"""

    res = execute_sql_scripts(query_sql)

    write_to_file("select_small_ave", res)
    return jsonify(res)


@app.route('/biggest_diff')
def biggest_diff() -> json:
    """Top 5 rooms with the biggest difference in student age"""
    query_sql = """ SELECT json_build_object(t1.name, (MAX(t2.birthday) - MIN(t2.birthday)))
                    FROM rooms AS t1
                    INNER JOIN students as t2 ON t1.id = t2.room 
                    GROUP BY t1.id
                    ORDER BY (MAX(t2.birthday) - MIN(t2.birthday)) DESC
                    LIMIT 5;"""

    res = execute_sql_scripts(query_sql)

    write_to_file("select_biggest_diff", res)
    return jsonify(res)


@app.route('/diff_sex')
def diff_sex() -> json:
    """list of rooms where students of different sexes live."""

    query_sql = """ SELECT t1.name
                    FROM rooms AS t1
                    INNER JOIN students as t2 ON t1.id = t2.room 
                    WHERE (SELECT COUNT(sex) FROM students WHERE sex='F') > 0
                    AND (SELECT COUNT(sex) FROM students WHERE sex='M') > 0
                    GROUP BY t1.id
                    ORDER BY t1.id;"""

    res = execute_sql_scripts(query_sql)

    write_to_file("select_sex", res)
    return jsonify(res)


@app.route('/index')
def index() -> str:
    """Creating indexes for columns birthday, sex, room in students table"""

    query_sql = """CREATE INDEX IF NOT EXISTS index_students ON students(birthday, sex, room);"""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query_sql)

    return ("Done!!!")


@app.route('/delete_index')
def delete_index() -> str:
    """Delete indexes"""

    query_sql = """DROP INDEX index_students;"""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query_sql)

    return ("Done!!!")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)