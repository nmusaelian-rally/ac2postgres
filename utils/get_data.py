import psycopg2
from psycopg2.extensions import AsIs
import yaml
import pytest

'''
to list of db tables in terminal:
    bash-3.2$ psql -d rally
    rally=# \d
'''

with open('../config.yaml', 'r') as file:
    config = yaml.load(file)

DB   = config["db"]["name"]
USER = config["db"]["user"]
PASS = config["db"]["password"]
HOST = config["db"]["host"]
PORT = config["db"]["port"]


def get_tables(dbname=DB):
    con = None # to avoid "local variable con might be referenced before assignement
    tables = []
    try:
        con = psycopg2.connect(database=dbname, user=USER, password=PASS, host=HOST, port=PORT)
        cur = con.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")

        cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        for table in cur.fetchall():
            tables.append(table)

    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)
    finally:
        if con:
            con.close()
            return tables


def test_get_tables():
    tables = get_tables()
    assert len(tables)
    print("\n-----\n")
    for t in tables:
        print(t[0]) # printing first element of each tuple, e.g. defect of (defect,)