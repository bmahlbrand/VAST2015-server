import pysolr
import pdb
import sys

class SolrIndexer():

    solr = None

    def __init__(self, solr_core):
        self.solr = pysolr.Solr('http://localhost:8983/solr/' + solr_core + '/', timeout=10)
        print('init solr connector')

    def add_traj(self, tuples):
        if self.solr != None:
            try:
                arr = []
                #(id, time, type, x, y)
                for t in tuples:
                    arr.append({
                        "id" : t[0],
                        "time" : t[1],
                        "x" : t[3],
                        "y" : t[4],
                        "type" : t[2]
                    })

                self.solr.add(arr)

            except:
                print ("Unexpected error:", sys.exc_info()[0])
                raise
            return True
        return False

    def add_com(self, tuples):
        if self.solr != None:
            try:
                arr = []
                #(from_id, to_id, time, location)
                for t in tuples:
                    arr.append({
                        "from" : t[0],
                        "to" : t[1],
                        "time" : t[2],
                        "location" : t[3]
                    })

                self.solr.add(arr)

            except:
                print ("Unexpected error:", sys.exc_info()[0])
                raise
            return True
        return False