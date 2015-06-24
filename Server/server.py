from bottle import Bottle, route, run, request, response, static_file, json_dumps
import urllib.parse
import pdb
import pickle
import traceback
app = Bottle()

from searcher import SolrSearcher
from DataManager import DataManager
import TimeFunc
import subprocess
import os
from lib import kde
from lib import kmeans
import base64
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


@app.route('/trajKDE', method="GET")
def query():
    dic = urllib.parse.parse_qs(request.query_string)
    start_date = None
    end_date = None
    t = None
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
        if key == 'type':
            t = ''.join(dic[key])
            
    results = None
    
    print('fetching data')
    try:
        if start_date is None:
            raise ValueError('You must specify start date')
#             start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            raise ValueError('You must specify end date')
#             end_date = '2014-6-09T23:59:00Z'
        
        if t is None:
            raise ValueError('You must specify movement type')
        
        rst = dataManager.collect_range_traj_locations(start_date, end_date, t)
        print("Saw {0} result(s).".format(len(rst)))
        print("...kde initializing")
        results = kde.KDE(rst,0,102,0,102)
        print("kde done...")
        results = results.tolist()
            
    except ValueError as err:
        print("wrong parameters passed")
        response.status = 400
        return err.args
    
    except:
        
        print("parse error")
        print(traceback.format_exc())
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
        
#         os.path.isfile('lib\\traj_cluster\\target.tra')
#         os.path.isfile('lib\\traj_cluster\\cluster.tra')
        
        # ret = dataManager.as_user_collection(results)
        # write ret to file1
        
#         try:
#             #os.chdir('lib\traj_cluster')
#             #subprocess.call(['"TraClus.exe"'], target.tra, cluster.tra, 25, 3)
#         except:
#             print("trajectory clustering failed")
#             print(traceback.format_exc())

    except:
        
        print("parse error")
        print(traceback.format_exc())
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

@app.route('/comUserTemporal', method="GET")
def query():
    print('requested')
    dic = urllib.parse.parse_qs(request.query_string)
    
    start_date = None
    end_date = None
    id = None
    loc = None
    
    for key in dic:
        if key == 's':
            start_date = ''.join(dic[key])
        if key == 'e':
            end_date = ''.join(dic[key])
        if key == 'id':
            id = ''.join(dic[key])
        if key == 'loc':
            loc = ''.join(dic[key])
        
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
        
        if loc is None:
            loc = '*'
        
        results = solr.query_com_ids(id.split(), start_date, end_date, loc)
        
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

@app.route('/kmeans', method="GET")
def query():
    print('requested')
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
#     s_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-06T08:00:00Z')
#     e_converted = TimeFunc.time_func_solr_date_to_python_date('2014-6-08T23:59:00Z')

        if start_date is None:
            start_date = '2014-6-05T08:00:00Z'
        
        if end_date is None:
            end_date = '2014-6-09T23:59:00Z'

        date_str = None
        
        if start_date.startswith("2014-06-06"):
            date_str = "friday"
        elif start_date.startswith("2014-06-07"):
            date_str = "saturday"
        elif start_date.startswith("2014-06-08"):
            date_str = "sunday"
        
        rst = solr.query_traj_checkin_kde(start_date, end_date)
        results = kmeans.kmeans(date_str, rst)
        
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

run(app, host='128.46.137.79', port=8093)
# run(app, host='localhost', port=8000)