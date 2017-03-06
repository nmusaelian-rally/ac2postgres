import sys
from dbconnector import DBConnector

class DBConnectorRunner:
    def __init__(self, config):
        self.dbconnector = DBConnector(config)

    def run(self):
        try:
            self.dbconnector.create_tables_n_columns()
            self.dbconnector.get_init_data()
        except Exception as msg:
            sys.stderr.write('Oh noes!\n %s' % msg)
            sys.exit(1)
        finally:
            self.dbconnector.db.close()
