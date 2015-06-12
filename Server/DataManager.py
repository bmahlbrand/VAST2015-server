import FileFunc
from index_file import parse_com, parse_traj
import sys
from datetime import datetime

class DataManager(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
#         self.commTable = FileFunc.
#         self.trajTable =
        print("initializing DataManager") 
        self.communicationFiles = list(["Data/comm-data-Fri.csv", "Data/comm-data-Sat.csv", "Data/comm-data-Sun.csv"])
        self.trajectoryFiles = list(["Data/park-movement-Fri.csv", "Data/park-movement-Sat.csv", "Data/park-movement-Sun.csv"])
            
    def read_communication_data(self):
        rst = []
        for filename in self.communicationFiles:
            with open(filename, encoding="utf-8") as f:
                next(f)
                for line in f:
                    line = line.strip()
                    rst.append(parse_com(line))
                f.close()
        return rst            
        
    def read_trajectory_data(self):
        rst = []
        for filename in self.trajectoryFiles:
            with open(filename, encoding="utf-8") as f:
                next(f)
                for line in f:
                    line = line.strip()
                    rst.append(parse_traj(line))
                f.close()
                    
        return rst

    def _compute_seconds(self, timestamp):
        spl = timestamp.split(" ")
        date = spl[0]
        time = spl[1]
        
        dateVal = date.split("-")
        val = time.split(":")
        
        secs = 60 * int(val[2]) + 60 * int(val[1]) + int(val[0])
        
        return secs
    
    def compute_index_from_time_comm(self, time):
        commStart = "2014-6-06 08:03:19"
        return self._compute_seconds(commStart) - self._compute_seconds(time)
    
    def compute_index_from_time_traj(self, time):
        trajStart = "2014-6-06 08:00:16"
        return self._compute_seconds(trajStart) - self._compute_seconds(time)
    
if __name__ == '__main__':
    data = DataManager()
    data.compute_index_from_time()
    data.read_communication_data()
    data.read_trajectory_data()

    print("DataManager loaded")