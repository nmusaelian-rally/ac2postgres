import sys
from playground.dbcon import DBCon

class DBConRunner():
    def __init__(self, args):
        config = args[0]
        self.dbcon = DBCon(config)

    def run(self, args):
        action = args[1]
        try:
            if action   == 'create':
                self.dbcon.create_tables_n_columns()
                self.dbcon.insert_init_data()
            elif action == 'update':
                self.dbcon.update()
            else:
                print('invalid action')
        except Exception as msg:
            sys.stderr.write('Oh noes!\n %s' % msg)
            sys.exit(1)
        finally:
            # finally is executed regardless of whether the statements in the try block fail or succeed.
            # else is executed only if the statements in the try block don't raise an exception.
            self.dbcon.db.close()

            # pg_connection_status is implemented using PQstatus.
            # psycopg doesn't expose that API, so the check is not available.
            # The only two places psycopg calls PQstatus itself is when a new connection is made, and at the beginning of execute.
            # You will need to issue a simple SQL statement to find out whether the connection is still there.