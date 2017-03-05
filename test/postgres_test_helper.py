import psycopg2
import yaml
import pytest
import os


TEMPLATE_CONFIG = 'configs/db_template.yml'
KEEP_CONFIG     = 'configs/db_keep.yml'
TEMP_DB  = 'temp'
KEEP_DB  = 'foobar'

class PostgresSuper:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
        self.conn = self.connect()
        self.cursor = self.conn.cursor()

    def read_config(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.load(file)
        return config

    def connect(self):
        db   = self.config['db']['name']
        user = self.config['db']['user']
        pasw = self.config['db']['password']
        host = self.config['db']['host']
        port = self.config['db']['port']
        try:
            conn = psycopg2.connect(database=db, user=user, password=pasw, host=host, port=port)
        except psycopg2.OperationalError as e:
            print(e)
            raise
        return conn

class PostgresCreatorDestroyer (PostgresSuper):
    def __init__(self, TEMPLATE_CONFIG):
        PostgresSuper.__init__(self, TEMPLATE_CONFIG)

    def create_db(self, dbname):
        # use set_isolation_level(0) to avoid psycopg2.InternalError: CREATE DATABASE cannot run inside a transaction block
        try:
            self.conn.set_isolation_level(0)
            self.cursor.execute('CREATE DATABASE ' + dbname)
            self.cursor.close()
        except psycopg2.ProgrammingError as e:
            print (e)
            raise
        return True

    def drop_db(self, dbname):
        try:
            self.conn.set_isolation_level(0)
            self.cursor.execute('DROP DATABASE ' + dbname)
            self.cursor.close()
        except psycopg2.ProgrammingError as e:
            print (e)
            raise
        return True

    def db_exists(self, dbname):
        # cursor.execute executes query but doesn’t return data. None is returned.
        # If you want to retrieve query result you have to use one of the fetch* methods
        sql = "SELECT 1 from pg_database WHERE datname='%s';" % dbname
        self.cursor.execute(sql)
        return self.cursor.fetchone()

class PostgresTestHelper (PostgresSuper):
    def __init__(self, config_file):
        PostgresSuper.__init__(self, config_file)

    def read_config(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.load(file)
        return config

    def connect(self):
        db   = self.config['db']['name']
        user = self.config['db']['user']
        pasw = self.config['db']['password']
        host = self.config['db']['host']
        port = self.config['db']['port']
        try:
            conn = psycopg2.connect(database=db, user=user, password=pasw, host=host, port=port)
        except psycopg2.OperationalError as e:
            print(e)
            raise
        return conn

    def create_table(self, table_name):
        try:
            self.cursor.execute("CREATE TABLE %s ();" %table_name)
        except psycopg2.Error as e:
            print(e)
            raise
        except:
            print("Oh noes!")

    def create_columns(self, table_name):
        try:
            for attr in attributes:
                self.cursor.execute("ALTER TABLE %s ADD COLUMN %s %s" %(table_name, attr['name'], attr['type']))
        except psycopg2.Error as e:
            print(e)
            raise
        except:
            print("Oh noes!")

    def copy_to_db(self, table_name):
        file_name = "csv/%s.csv" % table_name
        full_path = '%s/%s' % (os.getcwd(), file_name)
        try:
            with open(file_name, 'r', newline='') as f:
                sql = "COPY %s FROM '%s' CSV;" % (table_name, full_path)
                self.cursor.execute(sql, f)
            print("Inserted %s rows in %s table" % (self.cursor.rowcount, table_name))
        except psycopg2.Error as e:
            print(e)
        finally:
            self.conn.commit()

    def get_tables(self):
        tables = []
        try:
            self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [table for table in self.cursor.fetchall()]
        except psycopg2.Error as e:
            print(e)
        finally:
            return tables

    def get_columns(self, table):
        columns = []
        try:
            self.cursor.execute("SELECT * FROM %s; " % table)
            columns = [desc[0] for desc in self.cursor.description]
        except psycopg2.Error as e:
            print(e)
        finally:
            return columns

    def drop_all_tables(self):
        sql = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            print("\ndropping table: ", row[1])
            self.cursor.execute("drop table " + row[1] + " cascade")

    def truncate_table(self,table_name):
        try:
            print("truncate: ", table_name)
            self.cursor.execute("truncate table " + table_name + " cascade")
            return True
        except psycopg2.Error as e:
            print(e)


def test_create():
    dbname = TEMP_DB
    creator = PostgresCreatorDestroyer(TEMPLATE_CONFIG)
    if creator.db_exists(dbname):
        problem = 'database "temp" already exists'
        with pytest.raises(psycopg2.ProgrammingError) as excinfo:
            creator.create_db(dbname)
        assert problem in str(excinfo.value)
    else:
        success = creator.create_db(dbname)
        assert success
    creator.conn.close()

def test_drop():
    dbname    = TEMP_DB
    destroyer = PostgresCreatorDestroyer(TEMPLATE_CONFIG)
    if destroyer.db_exists(dbname):
        success = destroyer.drop_db(dbname)
        assert success
    else:
        problem = 'database "temp" does not exist'
        with pytest.raises(psycopg2.ProgrammingError) as excinfo:
            destroyer.drop_db(dbname)
        assert problem in str(excinfo.value)
    destroyer.cursor.close()

######################################################################

attributes = [
    {
        'name':'oid',
        'type':'bigint'
    },
    {
        'name':'name',
        'type':'text'
    },
    {
        'name':'fid',
        'type':'text'
    }]

def test_connect():
    dbname = KEEP_DB
    config = KEEP_CONFIG
    creator = PostgresCreatorDestroyer(TEMPLATE_CONFIG)
    if creator.db_exists(dbname) is None:
        creator.create_db(dbname)

    helper = PostgresTestHelper(config)
    assert helper.conn

    tables = helper.get_tables()
    if len(tables) > 0:
        helper.drop_all_tables()

    entities = helper.config['db']['tables'].replace(',', ' ').split()
    for entity in entities:
        helper.create_table(entity)
        helper.create_columns(entity)
        helper.copy_to_db(entity)
    tables = helper.get_tables()
    assert len(tables) == 2
    for t in tables:
        print("\n %s" %t[0]) # printing first element of each tuple, e.g. defect of (defect,)
        columns = helper.get_columns(t[0])
        assert len(columns)
        for column in columns:
            print("        %s" %column)

    helper.cursor.close()
    helper.conn.close()

def test_db_doesnot_exist():
    config_file = KEEP_CONFIG
    problem = 'database "unicorn" does not exist'
    with pytest.raises(psycopg2.OperationalError) as excinfo:
        PostgresTestHelper(config_file)
    assert problem in str(excinfo.value)


def test_bad_server():
    config_file = 'configs/db_bad_server.yml'
    problem = 'Connection refused'
    with pytest.raises(psycopg2.OperationalError) as excinfo:
        PostgresTestHelper(config_file)
    assert problem in str(excinfo.value)

def test_bad_user():
    config_file = 'configs/db_bad_user.yml'
    problem = 'password authentication failed for user "lenin"'
    with pytest.raises(psycopg2.OperationalError) as excinfo:
        PostgresTestHelper(config_file)
    assert problem in str(excinfo.value)


