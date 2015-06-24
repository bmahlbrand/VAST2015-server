from datetime import datetime
from Indexer import SolrIndexer
import sys

def time_func_solr_date_to_python_date(str):
        return datetime.strptime(str, "%Y-%m-%d %H:%M:%S")

def parse_traj(line):
    
    try:
        tokens = line.split(',')
        time = time_func_solr_date_to_python_date(tokens[0])
        id = int(tokens[1])
    
        type = True
    
        if tokens[2] == 'check-in':
            type = True
        elif tokens[2] == 'movement':
            type = False
        else:
            raise
    
        x = int(tokens[3])
        y = int(tokens[4])
        
        duration = 0
        if tokens[6] == 'NA':
            duration = -1
        else:
            duration = int(tokens[6])
    
        return (id, time, type, x, y, duration)
    except:
        print("error", line)
        return (None, None, None, None, None, None);

def index_traj_checkin(file_name, solr_indexer):
    
    rst = []
    
    with open(file_name, encoding="utf-8") as infile:
        
        for line in infile:
            
            line = line.strip()
            
            id, time, type, x, y, duration = parse_traj(line)
                
            if id != None:
                rst.append((id, time, type, x, y, duration))
            
            if len(rst) >= 100000:
                
                solr_indexer.add_traj(rst)
                print("indexing")
                rst= []
    
    if len(rst) > 0:
        solr_indexer.add_traj(rst)

if __name__ == '__main__':
    
    solr_indexer = SolrIndexer("VASTChallenge_traj_checkin")
    index_traj_checkin('data/kmeans/Friday_Check_Ins.csv', solr_indexer=solr_indexer);
    #index_traj_checkin('data/kmeans/Saturday_Check_Ins.csv', solr_indexer=solr_indexer);
    index_traj_checkin('data/kmeans/Sunday_Check_Ins.csv', solr_indexer=solr_indexer);
    

