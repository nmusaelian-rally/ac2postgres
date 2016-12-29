import sys
import json
import psycopg2
from psycopg2.extensions import AsIs
import yaml
from datetime import datetime, timezone


class DBCon:
    def __init__(self, config):
        self.config     = self.read_config(config)
        self.db         = self.connect_db()
        self.cursor     = self.db.cursor()
        self.entities   = self.config['db']['tables'].replace(',','').split()
        self.schema     = self.get_schema()
        self.index      = 0
        self.columns = {}

    def read_config(self, config_name):
        with open(config_name, 'r') as file:
            config = yaml.load(file)
        return config

    def read_data(self, file):
        with open(file) as json_data:
            d = json.load(json_data)
        return d['Results']

    def connect_db(self):
        errout = sys.stderr.write

        try:
            db = psycopg2.connect(database=self.config['db']['name'], user=self.config['db']['user'],
                password=self.config['db']['password'], host=self.config['db']['host'], port=self.config['db']['port'])
            return db
        except Exception as ex:
            errout(str(ex.args[0]))
            sys.exit(1)

    def get_schema(self):
        with open('meta.json') as json_data:
            m = json.load(json_data)
        return m['Results']


    def matchTypes(self,rally_type):
        return {
            'INTEGER' : 'bigint',
            'DATE'    : 'timestamp with time zone',
            'BOOLEAN' : 'boolean default false',
            'QUANTITY': 'double precision',  # e.g. Rally PlanEstimate's AttributeType: "QUANTITY"
            'STRING'  : 'text'
        }[rally_type]

    def attributes_subset(self, element):
        found = element['ElementName'] in self.config["db"]["columns"]
        return found


    def create_tables_n_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype['Attributes']))
            table_name = itemtype['Name']
            self.columns[table_name] = [{attr['ElementName']: attr['AttributeType']} for attr in attributes ]
            self.cursor.execute("CREATE TABLE %s ();", (AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN ID SERIAL PRIMARY KEY;",(AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN _start TIME WITH TIME ZONE;", (AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN _end TIME WITH TIME ZONE;", (AsIs(table_name),))
            for attr in attributes:
                element_name = attr['ElementName']
                attribute_type = attr['AttributeType']
                #allowed_values = attr.AllowedValues

                print('-' + element_name)
                print('---' + attribute_type)

                self.cursor.execute("ALTER TABLE %s ADD COLUMN %s %s",
                                (AsIs(table_name), AsIs(element_name), (AsIs(self.matchTypes(attribute_type))),))
            self.db.commit()

    def insert_init_data(self):
        for entity in self.entities:
            fields = [k for column in self.columns[entity] for k,v in column.items()]
            fetch = ','.join(fields)
            file_name = "%sz_%s.json" %(entity,self.index)
            response = self.read_data(file_name)
            for item in response:
                field_values = []
                formatters = ""
                empty_fields = []
                for field in fields:
                    value = item[field]
                    # RATING   type e.g. Severity     when empty return 'None'.
                    # QUANTITY type e.g. PlanEstimate when empty return None
                    if not value or value == 'None':
                        empty_fields.append(field)
                    else:
                        formatters = formatters + "%s,"
                        number_fields = [k for column in self.columns[entity] for k,v in column.items() if 'INTEGER' in column.values() or 'QUANTITY' in column.values()]
                        if field not in number_fields and field != 'None':
                            value = "'" + value + "'"
                        field_values.append(value)
                non_empty_fields = fields[:]
                for field in empty_fields:
                    non_empty_fields.remove(field)
                columns = ','.join(non_empty_fields)
                formatters = formatters[:-1] #remove trailing comma
                expression = "VALUES (%s)" % formatters % tuple(field_values)
                self.cursor.execute("INSERT INTO %s (%s) %s", (AsIs(entity), AsIs(columns), AsIs(expression),))
            self.cursor.execute("UPDATE %s SET _start = %s", (AsIs(entity), datetime.now(timezone.utc),))
        self.index += 1
        self.db.commit()
        self.db.close()