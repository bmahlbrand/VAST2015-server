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
from itertools import islice
from math import ceil
import json

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
		self.communicationFiles = list(["Data/comm-data-Fri.csv", "Data/comm-data-Sat.csv", "Data/comm-data-Sun.csv"])
# 		self.trajectoryFiles = list(["Data/park-movement-test.csv"])
		self.trajectoryFiles = list(["Data/park-movement-Fri.csv", "Data/park-movement-Sat.csv", "Data/park-movement-Sun.csv"])
		
		self.commStart = datetime.strptime("2014-6-06T08:03:19Z", "%Y-%m-%dT%H:%M:%SZ")
		self.trajStart = datetime.strptime("2014-6-06T08:00:16Z", "%Y-%m-%dT%H:%M:%SZ")

		self.commTable = None
		self.trajTable = None
		self.movementTable = None
		
# 		self.build_index()

		self.load_tables()
# 		self.movementTable = self._index_movement(5)
# 		self._serialize_movements()

		print("...DataManager initialized")

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
						# if t[2] is 0: #skip movements
						# 	continue
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
		s = self.compute_index_from_time_traj(start_time)
		e = self.compute_index_from_time_traj(end_time)
		rst = []
		
		if s < 0:
			s = self.compute_index_from_time_traj(self.trajStart)
		if e > 259200:
			e = 259200 
		
		while s < e:
			if self.trajTable[s] is not None:
				row = self.trajTable[s]
				rst.extend(row)
			s += 1
			
		return rst 
	
	def collect_range_traj_locations(self, start_time, end_time, t):
		s = self.compute_index_from_time_traj(start_time)
		e = self.compute_index_from_time_traj(end_time)

		rst = []

		if s < 0:
			s = self.compute_index_from_time_traj(self.trajStart)
		if e > 259200:
			e = 259200 

		if t == '*':
			while s < e:
				if self.trajTable[s] is not None:
					for row in self.trajTable[s]:
						rst.append([row[3], row[4]])
				s += 1
		elif t == '0': #movement
			while s < e:
				if self.trajTable[s] is not None:
					for row in self.trajTable[s]:
						if row[2] == False:
							rst.append([row[3], row[4]])
				s += 1
		elif t == '1': #check-in
			while s < e:
				if self.trajTable[s] is not None:
					for row in self.trajTable[s]:
						if row[2] == True:
							rst.append([row[3], row[4]])
				s += 1
		return rst
		
	def _chunkify(self, n, table):
		return [table[i:i + n] for i in range(0, len(table), n)]
	
	def _serialize_comm(self):
		print("serializing communications...")

		chunks = self._chunkify(1000, self.commTable)
		i = 0
		for chunk in chunks:
			if all(v is None for v in chunk) is True:
				continue
			i += 1
			with open("Data/seconds/communication/" + str(i) + ".json", 'wb') as fp:
				fp.write(bytes(json.dumps(chunk), 'UTF-8'))
		
		print("...communications serialized")
		
	#all trajectory movement/check-in
	def _serialize_traj(self):
		print("serializing trajectories...")
		
		chunks = self._chunkify(1000, self.trajTable)
		i = 0
		for chunk in chunks:
			if all(v is None for v in chunk) is True:
				continue
			i += 1
			with open("Data/seconds/trajectory/" + str(i) + ".json", 'wb') as fp:
				fp.write(bytes(json.dumps(chunk), 'UTF-8'))
				
		print("...trajectories serialized")
		
	#index for sampled movement of groups
	def _serialize_movements(self):
		print("serializing movements...")
		
		chunks = self._chunkify(1000, self.movementTable)
		i = 0
		for chunk in chunks:
			if all(v is None for v in chunk) is True:
				continue
			i += 1
			with open("Data/seconds/movement/" + str(i) + ".json", 'wb') as fp:
				fp.write(bytes(json.dumps(chunk), 'UTF-8'))
		
		print("...movements serialized")
		
	def serialize_tables(self):
		print("serializing tables...")
		self._serialize_comm()
		self._serialize_traj()
		self._serialize_movements()
		print("...tables serialized")
			
	def build_index(self):
		self.commTable = self.read_communication_data()
		self.trajTable = self.read_trajectory_data()
		self.movementTable = self._index_movement(5)
		self.serialize_tables()
		
	def _load_comm(self):
		print('loading communication data...')
		
		self.commTable = [None] * 259200 
		
		for file in os.listdir("Data/seconds/communication/"):
			with open("Data/seconds/communication/" + file, 'r') as fp:
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
					ind = row[0][1]
					self.trajTable[ind] = row
				
		print('...trajectory data loaded')
	
	def _load_movements(self):
		print('loading movement data...')
		
		self.movementTable = [None] * 259200 
		
		for file in os.listdir("Data/seconds/movement/"):
			with open("Data/seconds/movement/" + file, 'r') as fp:
				comm = json.load(fp)
				for row in comm:
					if row is None:
						continue
					ind = row[0][1]
					self.movementTable[ind] = row
		
		print('...movement data loaded')
		
	def load_tables(self):
		self._load_comm()
		self._load_traj()
		self._load_movements()
		
	def _sample_movement(self, movements, sampleRate):
		l = len(movements) 
		k = ceil(l / (sampleRate + 1))
		
		i = k
		rst = []
		
		while i < l:
			rst.append(movements[i])
			i += k
			
		return rst
	
	def _index_movement(self, sampleRate):
		print('indexing user movements...')
		
		rst = {}

		for entry in self.trajTable:
			
			if entry is None:
				continue
			
			for row in entry:
				userID = row[0]

				if userID not in rst.keys():
					rst[userID] = [[row[0], row[1], row[2], row[3], row[4]]]
				else:
					rst[userID].append([row[0], row[1], row[2], row[3], row[4]])
		
		print('mapped to users')
		
		ret = {}
		
		for id, user in rst.items():
			user_range = []
			seenCheckpoint = False
			
			for movement in user:
				if seenCheckpoint is False and movement[2] is True:
					seenCheckpoint = True
					
				elif seenCheckpoint is True and movement[2] is True:
					seenCheckpoint = False
					
					if id not in ret.keys():
						#add to array here
						ret[id] = self._sample_movement(user_range, sampleRate)
					else:	
						ret[id].extend(self._sample_movement(user_range, sampleRate))
					user_range = []
				else:
					user_range.append([movement[0], movement[1], movement[3], movement[4]])
		
		print("between check-in points sampled")
		
		timeTable = [None] * 259200 
		
		for row in ret.values():
			for entry in row: 
				timeInd = entry[1]
				if timeTable[timeInd] is None:
					timeTable[timeInd] = [entry]
				else:
					timeTable[timeInd].append(entry)
			
		print('...user movements indexed')
		return timeTable

	def write_movements(self, data):
		
		with open("lib/traj_cluster/target.tra", 'wb') as fp:

			fp.write(bytes("2" + '\n', 'UTF-8')) #dimensions
			fp.write(bytes(str(len(data)) + '\n', 'UTF-8'))
			lines = []
			i = 0
# 			print(data)
			for user in data:
				coords = ''
				
				for movements in user:
					coords += ' ' + str(movements[1]) + ' ' + str(movements[2])
				
				lines.append(str(i) + ' ' + str(len(user)) + coords)	
				i += 1
			
			for line in lines:
				fp.write(bytes(line + '\n', 'UTF-8'))

	def collect_range_group_traj(self, start_time, end_time, user_group, width, height):
		if height is None:
			height = 100
		if width is None:
			width = 100
			
		xScale = width / 100
		yScale = height / 100
		
		s = self.compute_index_from_time_traj(start_time)
		e = self.compute_index_from_time_traj(end_time)
		
		rst = {}
		
		if user_group == '*':
			while s < e:
				if self.movementTable[s] is not None:
					for row in self.movementTable[s]:
						userID = row[0]
		
						if userID not in rst.keys():
							rst[userID] = [[row[0], row[2] * xScale, row[3] * yScale]]
						else:
							rst[userID].append([row[0], row[2] * xScale, row[3] * yScale])
				s += 1			
		else:
			while s < e:
				if self.movementTable[s] is not None:
					for row in self.movementTable[s]:
						userID = row[0]
						if userID in user_group:
							if userID not in rst.keys():
								rst[userID] = [[row[0], row[2] * xScale, row[3] * yScale]]
							else:
								rst[userID].append([row[0], row[2] * xScale, row[3] * yScale])
				s += 1
		return rst.values()

if __name__ == '__main__':
	data = DataManager()
	
	# tests
	# http://localhost:8000/communicationTemporalFilter?s=2014-6-06T08:00:00Z&e=2014-6-06T08:10:00Z
	# http://localhost:8000/trajKDE?s=2014-6-06T08:00:16Z&e=2014-6-06T09:00:16Z&type=1