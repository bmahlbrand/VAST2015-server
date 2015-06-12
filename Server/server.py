from bottle import Bottle, route, run, request, response, static_file, json_dumps
import urllib.parse
import pdb
import traceback
app = Bottle()
from searcher import SolrSearcher
import TimeFunc
solr = SolrSearcher()

@app.hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS, GET'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

@app.route('/index', method="GET")
def hello():
    dic = urllib.parse.parse_qs(request.query_string)
    
    start_date = ''
    end_date = ''
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
    
    results = None
    
    print('fetching data')
#     try:
#     s_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-06T08:00:00Z')
#     e_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-08T23:59:00Z')
    
#     s = TimeFunc.time_func_python_date_to_solr_date(start_time)
#     e = TimeFunc.time_func_python_date_to_solr_date(end_time)
#     print(TimeFunc.time_func_python_date_to_solr_date(s_converted))
#     print(s_converted)
    s = '2014-6-06T08:00:00Z'
    e = '2014-6-08T23:59:00Z'
    #west lafayeet:
    #40.501903, -87.005587
    #40.347703, -86.737109
    
#         results = solr.search(True, '*', s_converted, e_converted, str(40.3), str(-87), str(40.5), str(-86.5))
#         results = solr.search(True, '*', s_converted, e_converted, str(39), str(-88), str(42), str(-84))
    solr.queryTrajectory(1591741, s, e, 1)

#     except:
#         print("parse error")
#         return json_dumps([]);
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results)

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
        
        if from_id is None:
            from_id = '*'
        
        if to_id is None:
            to_id = '*'
        
        if loc is None:
            loc = '*'
            
        start_date = '2014-6-05T08:00:00Z'
        end_date = '2014-6-09T23:59:00Z'
    
        results = solr.queryCommunication(from_id, to_id, start_date, end_date, loc)
    
    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([]);
    
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
        
        if id is None:
            id = '*'
        
        if type is None:
            type = '*'
        
        if type is '1':
            type = 'true'
        elif type is '0':
            type = 'false'
            
        results = solr.queryTrajectory(id, start_date, end_date, type)

    except:
        print(traceback.format_exc())
        print("parse error")
        return json_dumps([]);
    
    print('fetching done')
    response.content_type = 'application/json'
    return json_dumps(results, indent=2)

run(app, host='128.46.137.56', port=8093)