import json
import psycopg2
from flask import Flask, jsonify
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


app = Flask(__name__)


def write_from_sql(json_file, table_name):
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            with open(json_file) as json_file:
                data = json.load(json_file)
                query_sql = f""" insert into {table_name} 
                select * from json_populate_recordset(NULL::{table_name}, %s) """
                try:
                    cur.execute(query_sql, (json.dumps(data),))

                except psycopg2.errors.UniqueViolation as e:
                    print('Json has been read later\n', e)


def clear_table(table_name):
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            query_sql = f""" DELETE FROM {table_name}"""
            cur.execute(query_sql)


@app.route('/')
def home():
    return (
        '<a class="button" href="/wjson">Write to db from json file</a><br>'
        '<a class="button" href="/clear">Clear all table</a><br>'
        '<a class="button" href="/room_list">list of rooms</a><br>'
        '<a class="button" href="/small_ave">top 5 rooms with the smallest average age of students</a><br>'
        '<a class="button" href="/biggest_diff">top 5 rooms with the biggest difference in student age</a><br>'
        '<a class="button" href="/sex">different sex</a><br>'
    )


@app.route('/wjson')
def wjson():
    write_from_sql('json/rooms.json', 'rooms')
    write_from_sql('json/students.json', 'students')
    return ("Done!!!")


@app.route('/clear')
def clear():
    clear_table('rooms')
    clear_table('students')
    return ("Done!!!")


@app.route('/room_list')
def room_list():
    res = ''
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            query_sql = """SELECT json_build_object(t1.name, COUNT(t2.room))
                            FROM rooms AS t1
                            INNER JOIN students as t2 ON t1.id = t2.room 
                            GROUP BY t1.name, t2.room
                            ORDER BY room;"""
            cur.execute(query_sql)
            res = cur.fetchall()
            with open('json/select_room_list.json', 'w') as f:
                json.dump(res, f)
    return res


@app.route('/small_ave')
def small_ave():
    res = ''
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            query_sql = """ SELECT json_build_object(t1.name, SUM(age(t2.birthday))/COUNT(t2.birthday)) FROM rooms AS t1
                            INNER JOIN students as t2 ON t1.id = t2.room 
                            GROUP BY t1.name
                            ORDER BY SUM(age(t2.birthday))/COUNT(t2.birthday) LIMIT 5;"""
            cur.execute(query_sql)
            res = cur.fetchall()
            with open('json/select_small_ave.json', 'w') as f:
                json.dump(res, f)
    return res


@app.route('/biggest_diff')
def biggest_diff():
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            query_sql = """ SELECT json_build_object(t1.name, (MAX(t2.birthday) - MIN(t2.birthday)))
                            FROM rooms AS t1
                            INNER JOIN students as t2 ON t1.id = t2.room 
                            GROUP BY t1.name
                            ORDER BY (MAX(t2.birthday) - MIN(t2.birthday)) DESC
                            LIMIT 5;"""
            cur.execute(query_sql)
            res = cur.fetchall()
            with open('json/select_biggest_diff.json', 'w') as f:
                json.dump(res, f)
    return res


@app.route('/sex')
def sex():
    with psycopg2.connect(database="exampledb",
                          user="docker",
                          password="docker",
                          host='localhost',
                          port='5432') as conn:
        with conn.cursor() as cur:
            query_sql = """ SELECT t1.name
                            FROM rooms AS t1
                            INNER JOIN students as t2 ON t1.id = t2.room 
                            WHERE (SELECT COUNT(sex) FROM students WHERE sex='F') > 0
                            AND (SELECT COUNT(sex) FROM students WHERE sex='M') > 0
                            GROUP BY t1.id
                            ORDER BY t1.id;"""
            cur.execute(query_sql)
            res = cur.fetchall()
            with open('json/select_sex.json', 'w') as f:
                json.dump(res, f)
    return res


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)