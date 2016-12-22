import sys
import psycopg2
from psycopg2.extensions import AsIs
import yaml
import time
import dateutil.parser
from pyral import Rally, rallyWorkset, RallyRESTAPIError

errout = sys.stderr.write

with open('config.yml', 'r') as file:
    config = yaml.load(file)

conn = psycopg2.connect(database=config["db"]["name"], user=config["db"]["user"], password=config["db"]["password"], host=config["db"]["host"], port=config["db"]["port"])
print ("Opened database successfully")
cur = conn.cursor()


some_workitems = config['db']['tables'].replace(',','').split()
#some_attributes  = config["params"]["fetch"]
fields  = config["params"]["fetch"]
query = config['params']['query']

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

# def attributes_subset(element):
#     found = element.ElementName in some_attributes
#     return found

for workitem in some_workitems:
    response = rally.get('%s' % workitem, fetch=fields, query=query, order="ObjectID", pagesize=200)
    for item in response:
        # CreationDate,ObjectID,State,PlanEstimate,ScheduleState
        if workitem == 'Defect':
            cur.execute("INSERT INTO %s (%s) VALUES (%s, %s, %s, %s, %s)", \
                (AsIs(workitem), AsIs(fields), AsIs("'" + item.CreationDate + "'"), AsIs(item.ObjectID),
                 AsIs("'" + item.State + "'"), AsIs(item.PlanEstimate), AsIs("'" + item.ScheduleState + "'"),))
        if workitem == 'HierarchicalRequirement':
            cur.execute("INSERT INTO %s (creationdate,objectid,planestimate,schedulestate) VALUES (%s, %s, %s, %s)", \
                (AsIs(workitem), AsIs("'" + item.CreationDate + "'"), AsIs(item.ObjectID),
                 AsIs(item.PlanEstimate), AsIs("'" + item.ScheduleState + "'"),))

conn.commit()  
conn.close()


