from dbconnector import DBConnector
import psycopg2
from psycopg2.extensions import AsIs

def instantiate(config):
    return DBConnector(config)

config_file = 'config.yml'
conn = instantiate(config_file)


def get_tables():
    if conn.db:
        cur = conn.db.cursor()
        tables = []
        try:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [table for table in cur.fetchall()]
        except psycopg2.Error as e:
            print("oh, noes! " + e.pgerror)
        finally:
            return tables

def get_columns(table):
    if conn.db:
        cur = conn.db.cursor()
        columns = []
        try:
            cur.execute("SELECT * FROM %s; ", (AsIs(table),))
            columns = [desc[0] for desc in cur.description]
        except psycopg2.Error as e:
            print("oh, noes! " + e.pgerror)
        finally:
           return columns

def test_db_connection():
    db = conn.db
    assert db
    db.close()


def test_get_tables():
    tables = get_tables()
    assert len(tables)
    print("\n-----\n")
    for t in tables:
        print(t[0]) # printing first element of each tuple, e.g. defect of (defect,)
        columns = get_columns(t[0])
        assert len(columns)
        for column in columns:
            print("        %s" %column)
    conn.db.close()