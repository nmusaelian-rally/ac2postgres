import json

with open('data.json') as json_data:
    d = json.load(json_data)
print(d)

with open('meta.json') as json_data:
    m = json.load(json_data)
print(m)


def test_reading():
    assert int(m['TotalResultCount']) == 2
    assert len(d['Properties']) == 7
    assert len(d['Players']) == 2
    players = [player['Name'] for player in d['Players']]
    assert players[0] == 'Diana Dbag'
    assert players[1] == 'Dil Pickle'
