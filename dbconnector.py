import sys
import requests
import psycopg2
from psycopg2.extensions import AsIs
import yaml
from pyral import Rally, rallyWorkset, RallyRESTAPIError
from datetime import datetime, timezone

class DBConnector:
    def __init__(self, config):
        self.config     = self.read_config(config)
        self.ac         = self.connect_ac()
        self.db         = self.connect_db()
        self.cursor     = self.db.cursor()
        self.entities   = self.config['db']['tables'].replace(',','').split()
        self.schema     = self.get_schema()
        self.columns = {}
        self.cache_columns()

    def read_config(self, config_name):
        with open(config_name, 'r') as file:
            config = yaml.load(file)
        return config

    def connect_ac(self):
        errout    = sys.stderr.write

        USER      = self.config['ac'].get('user',None)
        PASS      = self.config['ac'].get('password',None)
        APIKEY    = self.config['ac'].get('apikey',None)
        URL       = self.config['ac']['url']
        WORKSPACE = self.config['ac']['workspace']
        PROJECT   = self.config['ac']['project']

        try:
            ac = Rally(URL, apikey=APIKEY, workspace=WORKSPACE, project=PROJECT)
            return ac
        except Exception as ex:
            errout(str(ex.args[0]))
            sys.exit(1)


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
        workitems_meta = []
        for entity in self.entities:
            workitems_meta.append(self.ac.typedef(entity))
        return workitems_meta

    def matchTypes(self,rally_type):
        return {
            'INTEGER' : 'bigint',
            'DATE'    : 'timestamp with time zone',
            'BOOLEAN' : 'boolean default false',
            'QUANTITY': 'double precision',  # e.g. Rally PlanEstimate's AttributeType: "QUANTITY"
            'STRING'  : 'text'
        }[rally_type]

    def convert_list_to_string_of_quoted_items(self, values):
        # Example of a list received as arg to this method:
        # ['Submitted', 'Open', 'Fixed', 'Closed']
        #
        # Example of a string that we need to pass to
        # ADD COLUMN %s text check (%s IN (%s) to populate IN (%s):
        # 'Submitted','Open','Fixed','Closed'

        str = ""
        for i, item in enumerate(values):
            str += "'" + item + "'"
            if i < len(values) - 1:
                str += ','
        return str

    def attributes_subset(self, element):
        found = element.ElementName in self.config["ac"]["fetch"]
        return found

    def cache_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype.Attributes))
            table_name = itemtype.ElementName
            self.columns[table_name] = [{attr.ElementName: attr.AttributeType} for attr in attributes ]

    def create_tables_n_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype.Attributes))
            table_name = itemtype.ElementName
            #populate list of dictionaries of column_name:type, e.g.
            # [{'CreationDate': 'DATE'}, {'ObjectID': 'INTEGER'}, {'ScheduleState': 'STATE'}]
            #self.columns[table_name] = [{attr.ElementName: attr.AttributeType} for attr in attributes ]
            self.cursor.execute("CREATE TABLE %s ();", (AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN ID SERIAL PRIMARY KEY;",(AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN _start TIME WITH TIME ZONE;", (AsIs(table_name),))
            self.cursor.execute("ALTER TABLE %s ADD COLUMN _end TIME WITH TIME ZONE;", (AsIs(table_name),))
            for attr in attributes:
                element_name = attr.ElementName
                attribute_type = attr.AttributeType
                allowed_values = attr.AllowedValues

                print('-' + element_name)
                print('---' + attribute_type)

                if attr.AttributeType == 'RATING':
                    rating_allowed_values = [a.StringValue for a in allowed_values]
                    self.cursor.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                                (AsIs(table_name), AsIs(element_name), AsIs(element_name),
                                 (AsIs(self.convert_list_to_string_of_quoted_items(rating_allowed_values))),))
                elif attr.AttributeType == 'STATE':
                    state_allowed_values = [a.StringValue for a in allowed_values]
                    self.cursor.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                                (AsIs(table_name), AsIs(element_name), AsIs(element_name),
                                 (AsIs(self.convert_list_to_string_of_quoted_items(state_allowed_values))),))
                else:
                    self.cursor.execute("ALTER TABLE %s ADD COLUMN %s %s",
                                (AsIs(table_name), AsIs(element_name), (AsIs(self.matchTypes(attribute_type))),))


            self.db.commit()

    def insert_init_data(self):
        query = self.config['ac']['query']
        for entity in self.entities:
            fields = [k for column in self.columns[entity] for k,v in column.items()]
            fetch = ','.join(fields)
            response = self.ac.get('%s' % entity, fetch=fetch, query=query, order="ObjectID", pagesize=200)
            for item in response:
                field_values = []
                formatters = ""
                empty_fields = []
                for field in fields:
                    value = getattr(item, field)
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
        self.db.commit()
        self.db.close()

    def update(self):
        query = self.config['ac']['query']
        for entity in self.entities:
            records_to_set_end = []
            new_row_oids = []
            second_cursor = self.db.cursor()
            fields = [k for column in self.columns[entity] for k, v in column.items()]
            fetch = ','.join(fields)
            response = self.ac.get('%s' % entity, fetch=fetch, query=query, order="ObjectID", pagesize=200)
            for item in response:
                field_values = []
                formatters = ""
                empty_fields = []
                for field in fields:
                    value = getattr(item, field)
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
                formatters = formatters[:-1]  # remove trailing comma
                expression = "VALUES (%s)" % formatters % tuple(field_values)
                for field in non_empty_fields:
                    # there will be multiple rows with the same name if there are multiple snapshots of the same records
                    # get current snapshots (_end IS NULL) for each items
                    # fetch id and each field one execution of SELECT command at a time for each record
                    self.cursor.execute("SELECT id, %s FROM %s WHERE _end IS NULL and ObjectID = '%s';",
                                        (AsIs(field), AsIs(entity), AsIs(item.ObjectID),))
                    # compare the value of the same field in the item from response with corresponding value in the db:
                    for row in self.cursor:
                        #print("comparing %s with %s" %(item[field], row[1]))
                        if getattr(item, field) != row[1]:
                            if getattr(item, 'ObjectID') in new_row_oids:
                                continue
                            new_row_oids.append(getattr(item, 'ObjectID'))
                            #print ("DIFF! record id %s" %row[0])
                            records_to_set_end.append(row[0])
                            second_cursor.execute("INSERT INTO %s (%s) %s RETURNING id;",(AsIs(entity), AsIs(columns), AsIs(expression),))
                            new_row_id = second_cursor.fetchone()[0]
                            print ("LAST inserted ID: %s" %new_row_id)
                            second_cursor.execute("UPDATE %s SET _start = %s WHERE id = %s",
                                     (AsIs(entity), datetime.now(timezone.utc), AsIs(new_row_id),))
                        continue
            for id in records_to_set_end:
                self.cursor.execute("UPDATE %s SET _end = %s WHERE id = %s", (AsIs(entity), datetime.now(timezone.utc), AsIs(id),))
        self.db.commit()
        self.db.close()

