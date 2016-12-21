'''
SELECT * FROM hierarchicalrequirement;
to confirm outcome
'''

# Truncate tablename CASCADE

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

def truncate_table(table_name):
    con = connect()
    cur = con.cursor()
    try:
        print("truncate: ", table_name)
        cur.execute("truncate table " + table_name + " cascade")
        con.close()
        return True
    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)

def test_truncate_table():
    table_name = "hierarchicalrequirement"
    result = truncate_table(table_name)
    assert result