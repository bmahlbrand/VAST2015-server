import pysolr
import pdb
import sys
import TimeFunc

class SolrSearcher:

    solr = None
    traj = None
    com = None
    
    def __init__(self):
#         self.solr = pysolr.Solr('http://128.46.137.79:8983/solr/VASTChallenge_com/', timeout=10)
        self.traj = pysolr.Solr('http://128.46.137.79:8983/solr/VASTChallenge_traj/', timeout=10)
        self.com = pysolr.Solr('http://128.46.137.79:8983/solr/VASTChallenge_com/', timeout=10)
        print('init solr connector')

    def search(self, start_time, end_time, id):

        filter_queries = [
            'time:[' + start_time + ' TO ' + end_time + ']',
            'id:' + id
        ]

        return_field = ['id', 'time', 'x', 'y', 'type']
        
        results = self.solr.search('*', fq=filter_queries, fl=return_field, rows=1000000)
        
        print("Saw {0} result(s).".format(len(results)))

        return results
    
    def queryCommunication(self, id_1, id_2, start_time, end_time, location):
#         start_time_str = TimeFunc.time_func_python_date_to_solr_date(start_time)
#         end_time_str = TimeFunc.time_func_python_date_to_solr_date(end_time)
        
        filter_queries = [
            'from_id:' + id_1,
            'to_id:' + id_2,
            'time:[' + start_time + ' TO ' + end_time + ']',
            'location:' + location
        ]
        
        return_field = ['time', 'from_id', 'to_id', 'location']
        
        results = self.com.search('*', fq=filter_queries, fl=return_field, rows=1000000, sort='time asc')
        
        print("Saw {0} result(s).".format(len(results)))
        
        rst = []
        
        for result in results:
            rst.append(result)
            
        return rst
    
    def queryTrajectory(self, id, start_time, end_time, type):
#         start_time_str = TimeFunc.time_func_python_date_to_solr_date(start_time)
#         end_time_str = TimeFunc.time_func_python_date_to_solr_date(end_time)
        
        filter_queries = [
            'time:[' + start_time + ' TO ' + end_time + ']',
            'id:' + id,
            'type:' + type
        ]
         
        return_field = ['id', 'time', 'x', 'y', 'type']
        
        results = self.traj.search('*', fq=filter_queries, fl=return_field, rows=1000000, sort='time asc')
        
        print("Saw {0} result(s).".format(len(results)))
        
        rst = []
        
        for result in results:
            rst.append(result)
            
        return rst
        
if __name__ == '__main__':
    solrSearcher = SolrSearcher()
#     s = TimeFunc.time_func_solr_date_to_python_date('2014-6-06T08:00:00Z')
#     e = TimeFunc.time_func_solr_date_to_python_date('2014-6-08T23:59:00Z')
    s = '2014-6-06T08:00:00Z'
    e = '2014-6-08T23:59:00Z'
    #west lafayeet:
    #40.501903, -87.005587
    #40.347703, -86.737109
    
#         results = solr.search(True, '*', s_converted, e_converted, str(40.3), str(-87), str(40.5), str(-86.5))
#         results = solr.search(True, '*', s_converted, e_converted, str(39), str(-88), str(42), str(-84))
    
    solrSearcher.queryCommunication('*', '*', s, e, '*')
    solrSearcher.queryTrajectory('564792', s, e, '*')
    
#     solrSearcher.search("2014-06-06T08:44:41Z", "2014-06-07T08:44:41Z", "365191")