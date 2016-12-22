import sys
import psycopg2
import yaml
import requests
from psycopg2.extensions import AsIs
from pyral import Rally, rallyWorkset, RallyRESTAPIError

errout = sys.stderr.write

with open('config.yml', 'r') as file:
    config = yaml.load(file)

conn = psycopg2.connect(database=config["db"]["name"], user=config["db"]["user"], password=config["db"]["password"], host=config["db"]["host"], port=config["db"]["port"])

print ("Opened database successfully")

some_workitems   = config["db"]["tables"]
some_attributes  = config["params"]["fetch"]
results = []


USER      = config["rally"]["user"]
PASS      = config["rally"]["password"]
APIKEY    = config["rally"]["apikey"]
URL       = config["rally"]["url"]
WORKSPACE = config["rally"]["workspace"]
PROJECT   = config["rally"]["project"]

try:
    rally = Rally(URL, apikey=APIKEY, workspace=WORKSPACE, project=PROJECT)
except Exception as ex:
    errout(str(ex.args[0]))
    sys.exit(1)

entities = some_workitems.replace(',','').split()
for entity in entities:
    results.append(rally.typedef(entity))

def matchTypes(rally_type):
    return {
        'INTEGER'   : 'bigint',
        'DATE'      : 'timestamp with time zone',
        'BOOLEAN'   : 'boolean default false',
        'QUANTITY'  : 'double precision'   # Rally PlanEstimate's AttributeType: "QUANTITY"
    }[rally_type]


def convert_list_to_string_of_quoted_items(values):
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

def attributes_subset(element):
    found = element.ElementName in some_attributes
    return found

cur = conn.cursor()

for result in results:
    attributes = list(filter(attributes_subset, result.Attributes))
    table_name = result.ElementName
    print (table_name)
    cur.execute("CREATE TABLE %s ();", (AsIs(table_name),))
    for attr in attributes:
        element_name   = attr.ElementName
        attribute_type = attr.AttributeType
        allowed_values = attr.AllowedValues
        
        print ('-' + element_name)
        print ('---' + attribute_type)
        
        if attr.ElementName == 'ObjectID':
             cur.execute("ALTER TABLE %s ADD COLUMN %s bigint PRIMARY KEY", (AsIs(table_name), AsIs(element_name),))
        elif attr.AttributeType == 'RATING':
            rating_allowed_values = [a.StringValue for a in allowed_values]
            cur.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                       (AsIs(table_name), AsIs(element_name), AsIs(element_name),(AsIs(convert_list_to_string_of_quoted_items(rating_allowed_values))),))
        elif attr.AttributeType == 'STATE':
            state_allowed_values = [a.StringValue for a in allowed_values]
            cur.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                       (AsIs(table_name), AsIs(element_name), AsIs(element_name),(AsIs(convert_list_to_string_of_quoted_items(state_allowed_values))),))
        else:
            cur.execute("ALTER TABLE %s ADD COLUMN %s %s", (AsIs(table_name), AsIs(element_name), (AsIs(matchTypes(attribute_type))),))
    conn.commit()