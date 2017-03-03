import os,sys
import psycopg2
from psycopg2.extensions import AsIs
import yaml
from pyral import Rally, rallyWorkset, RallyRESTAPIError
from datetime import datetime, timezone, timedelta
import time
import re
import csv

class DBConnector:
    def __init__(self, config):
        self.config     = self.read_config(config)
        self.ac         = self.connect_ac()
        self.db         = self.connect_db()
        self.cursor     = self.db.cursor()
        self.entities   = self.config['db']['tables'].replace(',', ' ').split()
        self.schema     = self.get_schema()
        self.columns = {}
        self.pi_states_map = {}
        self.cache_columns()
        self.init_data = {}

    def read_config(self, config_name):
        with open(config_name, 'r') as file:
            config = yaml.load(file)
        return config

    def connect_ac(self):
        errout    = sys.stderr.write

        USER      = self.config['ac'].get('user', None)
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

    # def get_entities(self):
    #     self.config['db']['tables'].replace(',', '').split()

    def get_schema(self):
        workitems_meta = []
        for entity in self.entities:
            workitems_meta.append(self.ac.typedef(entity))
        return workitems_meta

    def match_data_types(self,rally_type):
        return {
            'INTEGER'  : 'bigint'
            ,'DATE'    : 'timestamp with time zone'
            ,'BOOLEAN' : 'boolean default false'
            ,'QUANTITY': 'numeric'
            ,'DECIMAL' : 'numeric'
            ,'STRING'  : 'text'
            ,'TEXT'    : 'text'
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
        found = False
        result = re.search('\\b%s\\b' %element.ElementName, self.config["ac"]["fetch"])
        if result:
            found = True
        return found

    def cache_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype.Attributes))
            table_name = itemtype.ElementName
            #attr.SchemaType is needed, e.g. to identify User attr
            self.columns[table_name] = [{attr.ElementName: (attr.AttributeType, attr.SchemaType)} for attr in attributes]


    def get_pi_states(self, pi_name):
        fields = "TypeDef,Name,ObjectID"
        query = 'TypeDef.Name = %s' % pi_name
        response = self.ac.get('State', fetch=fields, query=query, pagesize=200)
        if response.resultCount:
            return  [state for state in response]

    def map_pi_states(self, pi_name):
        self.pi_states_map[pi_name] = []
        states = self.get_pi_states(pi_name)
        for state in states:
            self.pi_states_map[pi_name].append({state.ObjectID: state.Name})

    def get_username(self,oid):
        fields = 'ObjectID,UserName'
        query  = 'ObjectID = %s' % oid
        response = self.ac.get('User', fetch=fields, query=query, pagesize=200)
        if response.resultCount:
            return  [user for user in response][0]

    def table_name(self, entity):
        table = entity
        if table == 'User':
            table = 'Users'  # 'User' is a reserved table name in postgres
        return table


    def construct_sql(self, attr, itemtype):
        entity = itemtype.ElementName
        table = self.table_name(entity)

        _name   = attr.ElementName
        _type   = attr.AttributeType
        _values = attr.AllowedValues
        sql = ''

        if attr.AttributeType == 'RATING':
            rating_allowed_values = [a.StringValue for a in _values]
            sql = "ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) " \
                   %(table, _name, _name, self.convert_list_to_string_of_quoted_items(rating_allowed_values))
        elif attr.AttributeType == 'STATE':
            state_allowed_values = [a.StringValue for a in _values]
            sql = "ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) " \
                  %(table, _name, _name, self.convert_list_to_string_of_quoted_items(state_allowed_values))
        elif attr.AttributeType == 'OBJECT' and attr.SchemaType == 'User':
            if self.config['ac']['resolveUser']:
                sql = "ALTER TABLE %s ADD COLUMN %s %s" %(table, _name, 'text')
            else:
                sql = "ALTER TABLE %s ADD COLUMN %s %s" %(table, _name, 'bigint')
        elif attr.AttributeType == 'OBJECT' and attr.ElementName == 'State':
            self.map_pi_states(entity)
            state_allowed_values = [v for state in self.pi_states_map[entity] for k, v in state.items()]
            sql = "ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) " \
                  %(table, _name, _name, self.convert_list_to_string_of_quoted_items(state_allowed_values))
        elif attr.AttributeType == 'OBJECT':
            sql = "ALTER TABLE %s ADD COLUMN %s %s" %(table, _name, 'bigint')
        elif attr.ElementName == 'FormattedID':
            sql = "ALTER TABLE %s ADD COLUMN %s %s" %(table, _name, 'text')
        elif attr.AttributeType == 'COLLECTION':
            print('skipped %s because %s is not supported' %(_name, _type))
            pass
        else:
            sql = "ALTER TABLE %s ADD COLUMN %s %s" %(table, _name, self.match_data_types(_type))
        return sql

    def create_tables_n_columns(self):
        for itemtype in self.schema:
            attributes = list(filter(self.attributes_subset, itemtype.Attributes))
            entity = itemtype.ElementName
            table = self.table_name(entity)
            self.cursor.execute("CREATE TABLE %s ();" %table)
            for attr in attributes:
                sql = self.construct_sql(attr, itemtype)
                self.cursor.execute(sql)
            self.db.commit()


    def get_init_data(self):
        ac_start_times       =  {}
        ac_elapsed_times     =  {}
        export_start_times   =  {}
        export_elapsed_times =  {}
        db_start_times       =  {}
        db_elapsed_times      = {}
        query = self.config['ac']['query']
        records_per_workitem = {}
        for entity in self.entities:
            try:
                ac_start_times[entity] = time.time()
                fields = [k for column in self.columns[entity] for k, v in column.items()]
                ref_fields = [k for column in self.columns[entity] for k, v in column.items() if
                              (v[0] == 'OBJECT' and k != 'State' and v[1] != 'User')] # later when resolving user add this condition: and v[1] != 'User'
                pi_state_fields = [k for column in self.columns[entity] for k, v in column.items() if
                                   (v[0] == 'OBJECT' and k == 'State')]
                user_fields = [k for column in self.columns[entity] for k, v in column.items() if v[1] == 'User']
                collection_fileds = [k for column in self.columns[entity] for k, v in column.items()
                                     for v in column.values() if 'COLLECTION' in v]
                number_fields = [k for column in self.columns[entity] for k, v in column.items()
                                 for v in column.values() if 'INTEGER' in v or 'QUANTITY' in v or 'DECIMAL' in v]
                fetch = ','.join(fields)
                response = self.ac.get('%s' % entity, fetch=fetch, query=query, order="ObjectID", pagesize=2000, projectScopeDown=True)
                #print("result count for %s: %s"%(entity, response.resultCount))
                number_of_records = 0
                self.init_data[entity] = []
                try:
                    for item in response:
                        field_values = []
                        try:
                            for field in fields:
                                if field in collection_fileds:
                                    continue
                                value = getattr(item, field)
                                # RATING   type e.g. Severity     when empty return 'None'.
                                # QUANTITY type e.g. PlanEstimate when empty return None
                                if not value or value == 'None':
                                    value = None
                                else:
                                    if field in user_fields:
                                        if self.config['ac']['resolveUser']:
                                            value = value.Name
                                        else:
                                            value = int(value._ref.split('/')[-1])
                                    elif field in ref_fields:
                                        value = int(value._ref.split('/')[-1])
                                    elif field in pi_state_fields:
                                        oid = value._ref.split('/')[-1]
                                        value = [v for state in self.pi_states_map[entity] for k, v in state.items() if k == int(oid)][0]
                                    elif field == 'FormattedID' or (field not in number_fields and field != 'None'):
                                        value = "'" + str(value) + "'"
                                    elif field in collection_fileds:
                                        pass
                                field_values.append(value) #NOTE change of indent compare to commented out line above. I want to append None values
                        except:
                            e = sys.exc_info()[0]
                            print("Problem in fields loop, skipping item\n%s" %e)
                            continue
                        self.init_data[entity].append(tuple(field_values)) # example: {'Epic': [("'E863'", 4)]} where 4 is DirectChildrenCount
                except:
                    e = sys.exc_info()[0]
                    print("Problem in item loop, skipping item\n%s" %e)
                    continue
                records_per_workitem[entity] = {'Number of Fields': len(fields),'Number of Records': response.resultCount}
                ac_elapsed_times[entity] = time.time() - ac_start_times[entity]
                print("Time it took to get %s records from AC: %s, %s" % (entity, ac_elapsed_times[entity], timedelta(seconds=round(ac_elapsed_times[entity]))))
                export_start_times[entity] = time.time()
                self.save_init_data_to_csv(entity,fields)
                export_elapsed_times[entity] = time.time() - export_start_times[entity]
                print("Time it took to export %s records to a file: %s, %s" % (entity, export_elapsed_times[entity], timedelta(seconds=round(export_elapsed_times[entity]))))
                db_start_times[entity] = time.time()
                self.copy_to_db(entity)
                db_elapsed_times[entity] = time.time() - db_start_times[entity]
                #print("%s : %s" % (entity, records_per_workitem[entity]))
                print("Time it took to copy %s records to db: %s, %s" %  (entity, db_elapsed_times[entity], timedelta(seconds=round(db_elapsed_times[entity]))))
            except TypeError as e:
                print("Problem in entity loop\n%s" %e)
        self.db.commit()


    def save_init_data_to_csv(self, entity, fields):
        file_name = "%s.csv" %entity
        try:
            writer = csv.writer(open(file_name, "w"))
            for row in self.init_data[entity]:
                row = tuple([x.strip("'") if type(x).__name__ == 'str' else x for x in row])
                writer.writerow(row)
        except:
            e = sys.exc_info()[0]
            print("Problem in saving AC data to csv file\n%s" %e)


    def copy_to_db(self, entity):
        table_name = entity
        if entity == 'User':
            table_name = 'Users'
        file_name = "%s.csv" % entity
        full_path = '%s/%s' % (os.getcwd(), file_name)
        try:
            with open(file_name, 'r', newline='') as f:
                sql = "COPY %s FROM '%s' CSV;" % (table_name, full_path) #ok
                self.cursor.execute(sql, f)
            print("Inserted %s rows in %s table" % (self.cursor.rowcount, table_name))
        except psycopg2.Error as e:
            print("pycopg2 Problem in copying AC data to database\n%s" % e)

        self.cursor.execute("ALTER TABLE %s ADD PRIMARY KEY (objectid);" %table_name)