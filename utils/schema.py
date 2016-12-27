import sys
import yaml
from pyral import Rally, rallyWorkset, RallyRESTAPIError

errout = sys.stderr.write

with open('config.yml', 'r') as file:
    config = yaml.load(file)

some_workitems   = config['db']['tables']
some_attributes  = config['ac']['fetch']
results = []

USER      = config['ac']['user']
PASS      = config['ac']['password']
APIKEY    = config['ac']['apikey']
URL       = config['ac']['url']
WORKSPACE = config['ac']['workspace']
PROJECT   = config['ac']['project']

try:
    rally = Rally(URL, apikey=APIKEY, workspace=WORKSPACE, project=PROJECT)
except Exception as ex:
    errout(str(ex.args[0]))
    sys.exit(1)

def attributes_subset(element):
    found = element.ElementName in some_attributes
    return found

entities = some_workitems.replace(',','').split()
for entity in entities:
    results.append(rally.typedef(entity))


def test_results():
    for result in results:
        attributes = list(filter(attributes_subset, result.Attributes))
        table_name = result.ElementName # instead of 'Name' to avoid white space in 'Hierarchical Requirement'
        assert table_name in entities
        assert [attr.ElementName for attr in attributes if attr.ElementName == 'ObjectID'][0]

def test_rating_allowed_values():
    for result in results:
        attributes = list(filter(attributes_subset, result.Attributes))
        for attr in attributes:
            rating_allowed_values = []
            if attr.AttributeType == 'RATING':
                for a in attr.AllowedValues:
                    rating_allowed_values.append(a.StringValue)
                print(rating_allowed_values)
                if attr.ElementName == 'State':
                    assert 'Submitted' in rating_allowed_values
                if attr.ElementName == 'Severity':
                    assert 'Crash/Data Loss' in rating_allowed_values

def test_state_allowed_values():
    for result in results:
        attributes = list(filter(attributes_subset, result.Attributes))
        for attr in attributes:
            state_allowed_values = []
            if attr.AttributeType == 'STATE':
                for a in attr.AllowedValues:
                    state_allowed_values.append(a.StringValue)
                if attr.ElementName == 'ScheduleState':
                    assert 'In-Progress' in state_allowed_values
                print("%s %s allowed values: %s" % (result.ElementName, attr.ElementName, state_allowed_values))