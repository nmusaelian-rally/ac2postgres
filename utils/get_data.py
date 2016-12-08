import psycopg2
from psycopg2.extensions import AsIs
import yaml
import pytest

'''
to list of db tables in terminal:
    bash-3.2$ psql -d rally
    rally=# \d
'''

with open('config.yml', 'r') as file:
    config = yaml.load(file)

DB   = config["db"]["name"]
USER = config["db"]["user"]
PASS = config["db"]["password"]
HOST = config["db"]["host"]
PORT = config["db"]["port"]

def connect_db(dbname):
    con = None # to avoid "local variable con might be referenced before assignement
    try:
        con = psycopg2.connect(database=dbname, user=USER, password=PASS, host=HOST, port=PORT)
    except psycopg2.Error as e:
        print("oh, noes! " + e.pgerror)
    finally:
        return con


def get_tables(dbname):
    con = connect_db(dbname)
    if con:
        cur = con.cursor()
        tables = []
        try:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [table for table in cur.fetchall()]
        except psycopg2.Error as e:
            print("oh, noes! " + e.pgerror)
        finally:
            if con:
                con.close()
                return tables

def get_columns(dbname, table):
    con = connect_db(dbname)
    if con:
        cur = con.cursor()
        columns = []
        try:
            cur.execute("SELECT * FROM %s; ", (AsIs(table),))
            columns = [desc[0] for desc in cur.description]
        except psycopg2.Error as e:
            print("oh, noes! " + e.pgerror)
        finally:
            if con:
                con.close()
                return columns

def test_get_tables():
    tables = get_tables(DB)
    assert len(tables)
    print("\n-----\n")
    for t in tables:
        print(t[0]) # printing first element of each tuple, e.g. defect of (defect,)
        columns = get_columns(DB, t[0])
        assert len(columns)
        for column in columns:
            print("        %s" %column)

