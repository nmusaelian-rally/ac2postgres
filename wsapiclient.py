import requests
import yaml

class WsapiIteratorClient:
    def __init__(self, endpoint=None, url=None, user=None, password=None, witype=None, start=None, pagesize=None, workspace=None, fetch=None, query=None):
        with open('config.yml', 'r') as file:
            conf = yaml.load(file)
    
        self._endpoint  = endpoint or conf['connection']['endpoint']
        self._url       = url      or conf['connection']['url']
        self._user      = user     or conf['connection']['user']
        self._password  = password or conf['connection']['password']
        self._witype    = witype   or conf['connection']['witype']
        
        self._start     = start     or int(conf['params']['start'])
        self._pagesize  = pagesize  or int(conf['params']['pagesize'])
        self._workspace = workspace or conf['params']['workspace']
        self._fetch     = fetch     or conf['params']['fetch']
        self._query     = query     or conf['params']['query']
        
        
        self._total_result_count = 0
        
        self._results = []
        
    def __iter__(self): return self
    
    def next(self):
        if self._start > self._total_result_count + 1:
            raise StopIteration
        return self._make_request()
    
    __next__ = next

         
    def _make_request(self):
        params = {
            "workspace": "workspace/%s" %self._workspace,
            "query":     "%s" %self._query,
            "start":     "%s" %self._start,
            "pagesize":  "%s" %self._pagesize,
            "fetch":     "%s" %self._fetch
        }
        if self._endpoint == "workitem":
            r = requests.get("%s/%s" % (self._url,self._witype), params = params, auth=("%s" % self._user, "%s" % self._password)) 
        else:
            r = requests.get("%s" %self._url, auth=("%s" % self._user, "%s" % self._password)) 
        #print (r.url)
        data = r.json()['QueryResult']
        self._total_result_count = data['TotalResultCount']
        self._start += self._pagesize
        params['start'] = self._start
        return data['Results']