from dbconnector import DBConnector

def instantiate(config):
    return DBConnector(config)

config_file = 'config.yml'
conn = instantiate(config_file)

def test_ac_connection():
    ac = conn.ac
    assert ac.server == 'rally1.rallydev.com'
    assert ac.schema_url == 'https://rally1.rallydev.com/slm/schema/v2.0'

def test_schedule_state():
    prefixes = ['US','DE']
    entities = conn.config['db']['tables']           #  Defect, HierarchicalRequirement
    entities = entities.replace(',','').split()      # ['Defect', 'HierarchicalRequirement']
    for entity in entities:
        schema_item = conn.ac.typedef(entity)
        assert schema_item.IDPrefix in prefixes
        schedule_state_attr_type = [attr.AttributeType for attr in schema_item.Attributes if attr.ElementName == 'ScheduleState'][0]
        assert schedule_state_attr_type == 'STATE'

def test_defect_state():
    fields = conn.config['params']['fetch']  # CreationDate,ObjectID,State,PlanEstimate,ScheduleState
    entity = 'Defect'
    schema_item = conn.ac.typedef(entity)
    attributes = [attr for attr in schema_item.Attributes if attr.ElementName in fields]
    assert 'ObjectID' in [attr.ElementName for attr in attributes]
    assert 'State' in [attr.ElementName for attr in attributes]
    assert [attr.AttributeType for attr in schema_item.Attributes if attr.ElementName == 'State'][0] == 'RATING'
    state_allowed_values = [a.StringValue for attr in schema_item.Attributes if attr.ElementName == 'State'
                            for a in attr.AllowedValues]
    print (state_allowed_values) # ['Submitted', 'Open', 'Fixed', 'Closed']
    assert 'Fixed' in state_allowed_values
    state = [attr for attr in schema_item.Attributes if attr.ElementName == 'State'][0]
    assert state.Constrained == True

def test_creation_date():
    entity = 'Defect'
    schema_item = conn.ac.typedef(entity)
    creation_date = [attr for attr in schema_item.Attributes if attr.ElementName == 'CreationDate'][0]
    assert creation_date.AttributeType == 'DATE'
    assert creation_date.ReadOnly == True
    assert creation_date.Custom   == False
    assert creation_date.Constrained == False