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
    
        return (id, time, type, x, y)
    except:
        print("error", line)
        return (None, None, None, None, None);

def index_traj(file_name, solr_indexer):
    
    rst = []
    
    with open(file_name, encoding="utf-8") as infile:
        
        for line in infile:
            
            line = line.strip()
            
            id, time, type, x, y = parse_traj(line)
                
            if id != None:
                rst.append((id, time, type, x, y))
            
            if len(rst) >= 100000:
                
                solr_indexer.add_traj(rst)
                print("indexing")
                rst= []
    
    if len(rst) > 0:
        solr_indexer.add_traj(rst)
        
def parse_com(line):
    
    try:
        tokens = line.split(',')
        time = time_func_solr_date_to_python_date(tokens[0])
        from_id = int(tokens[1])
        
        to_id = 0
        if tokens[2] == 'external':
            to_id = -1
        else:
            to_id = int(tokens[2])
    
        location = -1
        
        if tokens[3] == 'Coaster Alley':
            location = 0
        elif tokens[3] == 'Entry Corridor':
            location = 1
        elif tokens[3] == 'Kiddie Land':
            location = 2
        elif tokens[3] == 'Tundra Land':
            location = 3
        elif tokens[3] == 'Wet Land':
            location = 4
        else:
            raise
    
        return (from_id, to_id, time, location)
    except:
        print("error", tokens[2], sys.exc_info())
        return (None, None, None, None);


#'Coaster Alley', 'Entry Corridor', 'Kiddie Land', 'Tundra Land', 'Wet Land';

def index_com(file_name, solr_indexer):
    
    rst = []
    
    with open(file_name, encoding="utf-8") as infile:
        
        for line in infile:
            
            line = line.strip()
            
            from_id, to_id, time, location = parse_com(line)
                
            if from_id != None:
                rst.append((from_id, to_id, time, location))
            
            if len(rst) >= 100000:
                
                solr_indexer.add_com(rst)
                print("indexing")
                rst= []
    
    if len(rst) > 0:
        solr_indexer.add_com(rst)
                

if __name__ == '__main__':
    
#     solr_indexer = SolrIndexer("VASTChallenge_traj")
#     index_traj('Data/park-movement-Fri.csv', solr_indexer=solr_indexer);
#     index_traj('Data/park-movement-Sat.csv', solr_indexer=solr_indexer);
#     index_traj('Data/park-movement-Sun.csv', solr_indexer=solr_indexer);
    solr_indexer = SolrIndexer("VASTChallenge_com")
    index_com('Data/comm-data-Fri.csv', solr_indexer=solr_indexer);
    index_com('Data/comm-data-Sat.csv', solr_indexer=solr_indexer);
    index_com('Data/comm-data-Sun.csv', solr_indexer=solr_indexer);
    

