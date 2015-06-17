import FileFunc
from index_file import parse_com, parse_traj
from TimeFunc import time_func_python_date_to_solr_date
import sys
from datetime import datetime, timedelta
from array import array
import traceback
import pickle
import os
import json
import pdb

def _getDate(dt):
	return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")

class DataManager(object):
	'''
	classdocs
	'''

	def __init__(self):
		'''
		Constructor
		'''
		print("initializing DataManager...")
# 		self.communicationFiles = list(["Data/comm-data-test.csv"])
# 		self.communicationFiles = list(["Data/comm-data-Fri.csv"])
		self.communicationFiles = list(["Data/comm-data-Fri.csv", "Data/comm-data-Sat.csv", "Data/comm-data-Sun.csv"])
		self.trajectoryFiles = list(["Data/park-movement-Fri.csv", "Data/park-movement-Sat.csv", "Data/park-movement-Sun.csv"])
		
		self.commStart = datetime.strptime("2014-6-06T08:03:19Z", "%Y-%m-%dT%H:%M:%SZ")
		self.trajStart = datetime.strptime("2014-6-06T08:00:16Z", "%Y-%m-%dT%H:%M:%SZ")

		self.commTable = None
		self.trajTable = None
		

		self.load_tables()
		# self.commTable = self.read_communication_data()
		# self.trajTable = self.read_trajectory_data()
		


		# self.serialize_tables()
# 		self.load_comm_data()
		# self.load_traj_data()

		print("...DataManager initialized")


	def load_comm_data(self):
		for filename in self.communicationFiles:
			filename = filename + ".pickle"
			fp = open(filename, 'rb')
			
			if self.commTable is None:
				self.commTable = pickle.load(fp)
			else:
				self.commTable = self.merge(self.commTable, pickle.load(fp))

				# self.commTable.extend(pickle.load(fp))
				
		print('...communication initialized')

	def load_traj_data(self):
		for filename in self.trajectoryFiles:
			filename = filename + ".pickle"
			fp = open(filename, 'rb')
			
			if self.trajTable is None:
				self.trajTable = pickle.load(fp)
			else:
				self.trajTable = self.merge(self.trajTable, pickle.load(fp))
		
		print('...trajectory initialized')

	def read_communication_data(self):
		rst = [None] * 259200
		for filename in self.communicationFiles:
			with open(filename, encoding="utf-8") as f:
				print(filename)
				next(f)
				for line in f:
					line = line.strip()
					t = parse_com(line)
			
					try:
						if t[0] is None or t[1] is None or t[2] is None or t[3] is None:
							raise TypeError
						
# 						time = time_func_python_date_to_solr_date(t[2])
						
						i = self.compute_index_from_time_comm(t[2])
						
						if rst[i] is None:
							rst[i] = [[t[0], t[1], i, t[3]]]
							# rst[i] = list(array('i', [t[0], t[1], i, t[3]]))
							# rst[i] = [{'from': t[0], 'to' : t[1], 'timestamp' : time, 'location' : t[3]}]
						else:
							rst[i].append([t[0], t[1], i, t[3]])
#                             rst[i].append(t)
							# rst[i].append({'from': t[0], 'to' : t[1], 'timestamp' : time, 'location' : t[3]})
					
					except TypeError:
						print("failed to build table for communique")
						print(traceback.format_exc())
					
					except IndexError:
						print("index out of bounds... @" + str(i))
			
# 				with open(filename + ".pickle", 'wb') as fp:
# 					pickle.dump(rst, fp)
# 				rst = [None] * 259200
		return rst            
		
	def read_trajectory_data(self):
		rst = [None] * 259200
		for filename in self.trajectoryFiles:
			with open(filename, encoding="utf-8") as f:
				print(filename)
				next(f)
				
				for line in f:
					line = line.strip()
					t = parse_traj(line)
					
					try:
						if t[0] is None or t[1] is None or t[2] is None or t[3] is None or t[4] is None:
							raise TypeError
						if t[2] is 0: #skip movements
							continue
# 						time = time_func_python_date_to_solr_date(t[1])
#                         t[1] = time_func_python_date_to_solr_date(t[1])
						i = self.compute_index_from_time_traj(t[1])

						if rst[i] is None:
# 							rst[i] = [t]
							rst[i] = [[t[0], i, t[2], t[3], t[4]]]
							# rst[i] = list(array('i', [t[0], i, t[2], t[3], t[4]]))
							# rst[i] = [{'id' : t[0], 'timestamp' : time, 'type' : t[2], 'x': t[3], 'y': t[4]}]
						else:
							rst[i].append([t[0], i, t[2], t[3], t[4]])
							# rst[i].append(array('i', [t[0], i, t[2], t[3], t[4]]))
#                             rst[i].append(t)
							# rst[i].append({'id' : t[0], 'timestamp' : time, 'type' : t[2], 'x': t[3], 'y': t[4]})
							
					except TypeError:
						print("failed to build table for trajectories")
					
					except IndexError:
						print("index out of bounds... @" + str(i))
				
# 				with open(filename + ".pickle", 'wb') as fp:
# 					pickle.dump(rst, fp)
# 				rst = [None] * 259200
				# rst = dict()
					
		return rst

	def compute_index_from_time_comm(self, time):
		if type(time) is str:
			time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
		return int((time - self.commStart).total_seconds())
	
	def compute_time_from_seconds_comm(self, seconds):
		return self.commStart + datetime.timedelta(seconds=seconds)
	
	def compute_index_from_time_traj(self, time):
		if type(time) is str:
			time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
		return int((time - self.trajStart).total_seconds())
	
	def compute_time_from_seconds_traj(self, seconds):
		return self.trajStart + datetime.timedelta(seconds=seconds)
	
	def collect_range_comm(self, start_time, end_time):
		s = self.compute_index_from_time_comm(start_time)
		e = self.compute_index_from_time_comm(end_time)
		rst = []
		
		if s < 0:
			s = 0
		if e > 259200:
			e = 259200  
		
		while s < e:
			if self.commTable[s] is not None:
				row = self.commTable[s]
				rst.extend(row)
			s += 1
			
		return rst
	
	def as_user_collection(self, results):
		
		rst = dict()

		for row in results:
			userID = row[0]
			if rst[userID] is None:
				rst[userID] = [{'x' : row[3], 'y' : row[4]}]
			else:
				rst[userID].append({'x' : row[3], 'y' : row[4]})

		return rst

	def collect_range_traj(self, start_time, end_time):
		s = self.compute_index_from_time_comm(start_time)
		e = self.compute_index_from_time_comm(end_time)
		rst = []
		
		if s < 0:
			s = self.compute_index_from_time_comm(self.trajStart)
		if e > 259200:
			e = 259200 
		
		while s < e:
			if self.trajTable[s] is not None:
				row = self.trajTable[s]
				rst.extend(row)
			s += 1
			
		return rst 
	
	def _chunkify(self, n, table):
		return [table[i:i + n] for i in range(0, len(table), n)]
		
	def _serialize_comm(self):
		chunks = self._chunkify(1000, self.commTable)
		i = 0
		for chunk in chunks:
# 			print(chunk)
			if all(v is None for v in chunk) is True:
				continue
# 			print(chunk[0][0][2])
			i += 1
			with open("Data/seconds/communication/" + str(i) + ".json", 'wb') as fp:
# 					pdb.set_trace()
					fp.write(bytes(json.dumps(chunk), 'UTF-8'))

	def _serialize_traj(self):
		chunks = self._chunkify(1000, self.trajTable)
		i = 0
		for chunk in chunks:
			if all(v is None for v in chunk) is True:
				continue
			i += 1
			with open("Data/seconds/trajectory/" + str(i) + ".json", 'wb') as fp:
					# pdb.set_trace()
					
					fp.write(bytes(json.dumps(chunk), 'UTF-8'))
	
	def serialize_tables(self):
		print("serializing tables...")
		self._serialize_comm()
		self._serialize_traj()
		print("...tables serialized")

	def _load_comm(self):
		print('loading communication data...')
		
		self.commTable = [None] * 259200 
		
		for file in os.listdir("Data/seconds/communication/"):
			with open("Data/seconds/communication/" + file, 'r') as fp:
				# ind = int(os.path.splitext(file)[0])
				# self.commTable[ind] = json.load(fp)
				comm = json.load(fp)
				for row in comm:
					if row is None:
						continue
					ind = row[0][2]
					self.commTable[ind] = row
				
		print('...communication data loaded')
	
	def _load_traj(self):
		print('loading trajectory data...')
		
		self.trajTable = [None] * 259200 
		
		for file in os.listdir("Data/seconds/trajectory/"):
			with open("Data/seconds/trajectory/" + file, 'r') as fp:
				comm = json.load(fp)
				for row in comm:
					if row is None:
						continue
					ind = row[0][2]
					self.trajTable[ind] = row
				
		print('...trajectory data loaded')

	def load_tables(self):
		
		self._load_comm()
		self._load_traj()

if __name__ == '__main__':
	data = DataManager()
	# tests
	# http://localhost:8000/communicationTemporalFilter?s=2014-6-06T08:00:00Z&e=2014-6-06T08:10:00Z