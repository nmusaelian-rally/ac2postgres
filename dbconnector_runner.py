from dbconnector import DBConnector

class DBConnectorRunner():
    def __init__(self, config):
        self.dbconnector = DBConnector(config)

    def create_tables_n_columns(self):
        self.dbconnector.create_tables_n_columns()
