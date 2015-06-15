from bottle import Bottle, route, run, request, response, static_file, json_dumps
import urllib.parse
import pdb
import traceback
app = Bottle()

from searcher import SolrSearcher
from DataManager import DataManager 
import TimeFunc

solr = SolrSearcher()
dataManager = DataManager()

@app.hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS, GET'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

@app.route('/communicationTemporalFilter', method="GET")
def query():
    dic = urllib.parse.parse_qs(request.query_string)
    start_date = None
    end_date = None
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
    
    results = None
    
    print('fetching data')
    try:
        if start_date is None:
            start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            end_date = '2014-6-09T23:59:00Z'
            
        results = dataManager.collect_range_comm(start_date, end_date)
    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([]);
    
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results, indent=2)

@app.route('/communications', method="GET")
def query():
    dic = urllib.parse.parse_qs(request.query_string)
    
    start_date = ''
    end_date = ''
    from_id = None
    to_id = None
    loc = None
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
        if key == 'loc':
            loc = ''.join(dic[key])
        if key == 'id1':
            to_id = ''.join(dic[key])
        if key == 'id2':
            from_id = ''.join(dic[key])
            
    results = None
    
    print('fetching data')
    try:
#     s_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-06T08:00:00Z')
#     e_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-08T23:59:00Z')
        if start_date is None:
            start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            end_date = '2014-6-09T23:59:00Z'
            
        if from_id is None or to_id is None or from_id is '*' or to_id is '*':
            raise ValueError('invalid id1 or id2')
        
        if loc is None:
            loc = '*'
            
        start_date = '2014-6-05T08:00:00Z'
        end_date = '2014-6-09T23:59:00Z'
    
        results = solr.queryCommunication(from_id, to_id, start_date, end_date, loc)
    
    except ValueError as err:
        print("wildcard passed")
        response.status = 400
        return err.args
    
    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([])
    
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results, indent=2)


@app.route('/trajectoryTemporalFilter', method="GET")
def query():
    dic = urllib.parse.parse_qs(request.query_string)
    start_date = None
    end_date = None
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
    
    results = None
    
    print('fetching data')
    try:
        if start_date is None:
            start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            end_date = '2014-6-09T23:59:00Z'
            
        results = dataManager.collect_range_traj(start_date, end_date)
    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([])
    
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results, indent=2)
   
@app.route('/trajectories', method="GET")
def query():
    dic = urllib.parse.parse_qs(request.query_string)
    
    start_date = None
    end_date = None
    id = None
    type = None
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
        if key == 'id':
            id = ''.join(dic[key])
        if key == 'type':
            type = ''.join(dic[key])
        
    results = None
    
    print('fetching data')
    try:
#     s_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-06T08:00:00Z')
#     e_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-08T23:59:00Z')

        if start_date is None:
            start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            end_date = '2014-6-09T23:59:00Z'
                   
        if id is None or id is '*':
            raise ValueError('You must specify an id')
        
        if type is None:
            type = '*'
        
        if type is '1':
            type = 'true'
        elif type is '0':
            type = 'false'
            
        results = solr.queryTrajectory(id, start_date, end_date, type)
        
    except ValueError as err:
        print("wildcard passed")
        response.status = 400
        return err.args
    
    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([]);
    
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results, indent=2)

# run(app, host='128.46.137.56', port=8093)
# server.start(app, host='localhost', port=8093)
run(app, host='localhost', port=8000)