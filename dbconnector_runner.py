import sys
from dbconnector import DBConnector

class DBConnectorRunner():
    def __init__(self, config):
        self.dbconnector = DBConnector(config)

    # def create(self):
    #     self.dbconnector.create_tables_n_columns()
    #     self.dbconnector.insert_init_data()

    def run(self, args):
        action = args[1]
        try:
            if action   == 'create':
                self.dbconnector.create_tables_n_columns()
                self.dbconnector.insert_init_data()
            elif action == 'update':
                self.dbconnector.update()
            else:
                print('invalid action')
        except Exception as msg:
            sys.stderr.write('Oh noes!\n %s' % msg)
            sys.exit(1)
        finally:
            self.dbconnector.db.close()
