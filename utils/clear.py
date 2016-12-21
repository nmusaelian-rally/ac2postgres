import psycopg2
import yaml

'''
to check outcomes in terminal:
    $ sudo su postgres
    bash-3.2$ psql -l
'''

with open('config.yml', 'r') as file:
    config = yaml.load(file)

USER = config["db"]["user"]
PASS = config["db"]["password"]
HOST = config["db"]["host"]
PORT = config["db"]["port"]

def drop_db(dbname):
    with psycopg2.connect(database="postgres", user=USER, password=PASS, host=HOST, port=PORT) as conn:
        with conn.cursor() as cur:
            conn.set_isolation_level(0)
            cur.execute('DROP DATABASE ' + dbname)
            cur.close()
    return True



def test_drop_db():
    dbname = config["db"]["name"]
    result = False
    try:
       result = drop_db(dbname)
    except psycopg2.Error as e:
        print ("oh, noes! " + e.pgerror)
    else:
        print("great success!")
    assert result


