import psycopg2
import yaml
import requests
from psycopg2.extensions import AsIs
from wsapiclient import WsapiIteratorClient

with open('config.yaml', 'r') as file:
    config = yaml.load(file)

conn = psycopg2.connect(database=config["db"]["name"], user=config["db"]["user"], password=config["db"]["password"], host=config["db"]["host"], port=config["db"]["port"])

print ("Opened database successfully")
    
endpoint  = "schema"
url       = config["connection"]["schema_url"]
user      = config["connection"]["user"]
password   = config["connection"]["password"]
some_workitems   = config["db"]["tables"]
some_attributes  = config["params"]["fetch"]
results = []

for page in WsapiIteratorClient(endpoint, url,user,password):
    for schema in page:
        results.append(schema)

#print (type(results))

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

def getRaitingAllowedValues(allowed_values_endpoint):
    endpoint  = "schema"
    url = allowed_values_endpoint
    string_values = []
    for values in WsapiIteratorClient(endpoint, url,user,password):
        for value in values:
            string_values.append(value['StringValue'])
    
    # (",".join([d['StringValue'] for d in av])) # will not work: it won't have quotes around items
    str = convert_list_to_string_of_quoted_items(string_values)
    return str

def getStates(allowed_values):
    string_values = [av['StringValue'] for av in allowed_values]
    str = convert_list_to_string_of_quoted_items(string_values)
    return str

def workitems_subset(element):
    found = element['_refObjectName'] in some_workitems
    return found

def attributes_subset(element):
    found = element['ElementName'] in some_attributes
    return found

cur = conn.cursor()

workitmes = list(filter(workitems_subset, results))
#print (len(results))
for i, result in enumerate(list(filter(workitems_subset, results))):
    attributes = list(filter(attributes_subset, result['Attributes']))
    table_name = result['_refObjectName'] #instead of 'Name' to avoid squishing/unsquishing 'Name' with spaces
    print (table_name)
    cur.execute("CREATE TABLE %s ();", (AsIs(table_name),))
    for i, attribute in enumerate(attributes):
        element_name   = attribute['ElementName']
        attribute_type = attribute['AttributeType']
        allowed_values = attribute['AllowedValues']
        
        print ('-' + element_name)
        print ('---' + attribute_type)
        
        if attribute['ElementName'] == 'ObjectID':
            cur.execute("ALTER TABLE %s ADD COLUMN %s bigint PRIMARY KEY", (AsIs(table_name), AsIs(element_name),))
        elif attribute['AttributeType'] == 'RATING':
           cur.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                       (AsIs(table_name), AsIs(element_name), AsIs(element_name),(AsIs(getRaitingAllowedValues(allowed_values['_ref']))),))
        elif attribute['AttributeType'] == 'STATE':
           cur.execute("ALTER TABLE %s ADD COLUMN %s text check (%s IN (%s)) ",
                       (AsIs(table_name), AsIs(element_name), AsIs(element_name),(AsIs(getStates(allowed_values))),))
        else:
            cur.execute("ALTER TABLE %s ADD COLUMN %s %s", (AsIs(table_name), AsIs(element_name), (AsIs(matchTypes(attribute_type))),))
    conn.commit()