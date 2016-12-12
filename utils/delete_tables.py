import psycopg2
import yaml

with open('config.yml', 'r') as file:
    config = yaml.load(file)

DB   = config["db"]["name"]
USER = config["db"]["user"]
PASS = config["db"]["password"]
HOST = config["db"]["host"]
PORT = config["db"]["port"]


def connect():
    con = None
    try:
        con = psycopg2.connect(database=DB, user=USER, password=PASS, host=HOST, port=PORT)
        con.set_isolation_level(0)
    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)
    return con

def drop_all_tables():
    con = connect()
    cur = con.cursor()
    try:
        cur.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cur.fetchall()
        print(tables)  # [('public', 'hierarchicalrequirement'), ('public', 'defect')]
        for table in tables:
            print ("dropping table: ", table[1])
            cur.execute("drop table " + table[1] + " cascade")
        con.close()
        return True
    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)

def drop_table(table_name):
    con = connect()
    cur = con.cursor()
    try:
        print("dropping table: ", table_name)
        cur.execute("drop table " + table_name + " cascade")
        con.close()
        return True
    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)


def test_drop_all_tables():
    result = drop_all_tables()
    assert result

def test_drop_table():
    table_name = "defect"
    result = drop_table(table_name)
    assert result
