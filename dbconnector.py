import sys
import requests
import psycopg2
from psycopg2.extensions import AsIs
import yaml
from pyral import Rally, rallyWorkset, RallyRESTAPIError

class DBConnector:
    def __init__(self, config):
        self.config     = self.read_config(config)
        self.ac         = self.connect_ac()
        self.db         = self.connect_db()
        self.cursor     = self.db.cursor()
        self.entities   = self.config["db"]["tables"].replace(',','').split()
        self.schema     = self.get_schema()
        self.columns = {}

    def read_config(self, config_name):
        with open(config_name, 'r') as file:
            config = yaml.load(file)
        return config

    def connect_ac(self):
        errout    = sys.stderr.write

        USER      = self.config["rally"]["user"]
        PASS      = self.config["rally"]["password"]
        APIKEY    = self.config["rally"]["apikey"]
        URL       = self.config["rally"]["url"]
        WORKSPACE = self.config["rally"]["workspace"]
        PROJECT   = self.config["rally"]["project"]

        try:
            ac = Rally(URL, apikey=APIKEY, workspace=WORKSPACE, project=PROJECT)
            return ac
        except Exception as ex:
            errout(str(ex.args[0]))
            sys.exit(1)


    def connect_db(self):
        errout = sys.stderr.write

        try:
            db = psycopg2.connect(database=self.config["db"]["name"], user=self.config["db"]["user"],
                password=self.config["db"]["password"], host=self.config["db"]["host"], port=self.config["db"]["port"])
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
            'QUANTITY': 'double precision'  # e.g. Rally PlanEstimate's AttributeType: "QUANTITY"
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
        found = element.ElementName in self.config["params"]["fetch"]
        return found


    def create_tables_n_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype.Attributes))
            table_name = itemtype.ElementName
            #populate list of dictionaries of column_name:type, e.g.
            # [{'CreationDate': 'DATE'}, {'ObjectID': 'INTEGER'}, {'ScheduleState': 'STATE'}]
            self.columns[table_name] = [{attr.ElementName: attr.AttributeType} for attr in attributes ]
            self.cursor.execute("CREATE TABLE %s ();", (AsIs(table_name),))
            for attr in attributes:
                element_name = attr.ElementName
                attribute_type = attr.AttributeType
                allowed_values = attr.AllowedValues

                print('-' + element_name)
                print('---' + attribute_type)

                if attr.ElementName == 'ObjectID':
                    self.cursor.execute("ALTER TABLE %s ADD COLUMN %s bigint PRIMARY KEY",
                                (AsIs(table_name), AsIs(element_name),))
                elif attr.AttributeType == 'RATING':
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
        query = self.config['params']['query']
        for entity in self.entities:
            fields = [k for column in self.columns[entity] for k,v in column.items()]
            fetch = ','.join(fields)

            response = self.ac.get('%s' % entity, fetch=fetch, query=query, order="ObjectID", pagesize=200)
            for item in response:
                field_values = []
                formatters = ""
                for field in fields:
                    value = getattr(item, field)
                    if value:
                        formatters = formatters + "%s, "
                        number_fields = [k for column in self.columns[entity] for k,v in column.items() if 'INTEGER' in column.values() or 'QUANTITY' in column.values()]
                        if field not in number_fields:
                            value = "'" + value + "'"
                        field_values.append(value)
                formatters = formatters[:-2]
                expression = "VALUES (%s)" % formatters % tuple(field_values)
                self.cursor.execute("INSERT INTO %s (%s) %s", (AsIs(entity), AsIs(fetch), AsIs(expression),))
        self.db.commit()
        self.db.close()