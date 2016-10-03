
from wsapiclient import WsapiIteratorClient
import psycopg2
from psycopg2.extensions import AsIs
import yaml
import time
import dateutil.parser

with open('config.yaml', 'r') as file:
    config = yaml.load(file)

conn = psycopg2.connect(database=config["db"]["name"], user=config["db"]["user"], password=config["db"]["password"], host=config["db"]["host"], port=config["db"]["port"])
print ("Opened database successfully")
cur = conn.cursor()

for page in WsapiIteratorClient():
    for story in page:
        #print ("ObjectID: %s, PlanEstimate: %s, CreationDate: %s, ScheduleState: %s" %(story['ObjectID'], story['PlanEstimate'], story['CreationDate'], story['ScheduleState'])) # 56425731020 (set PlanEst to 1 of US3)
        print ("inserting story %s..." %story['ObjectID'])
        cur.execute("INSERT INTO hierarchicalrequirement (creationdate,objectid,schedulestate,planestimate) VALUES (%s, %s, %s, %s)",\
                    (AsIs("'" + story['CreationDate'] + "'"),AsIs(story['ObjectID']),AsIs("'" + story['ScheduleState'] + "'"), AsIs(story['PlanEstimate']),))
    
conn.commit()  
conn.close()


