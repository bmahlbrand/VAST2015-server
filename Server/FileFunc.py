import os
import glob

class FileFunc:
	def read_file_into_list(filename):
		data = [line.strip() for line in open(filename, 'r')]
		return data
	
	def write_list_into_file_append(filename, list):
		with open(filename, 'a') as f:
			for s in list:
				f.write(s + '\n')
	
	def write_list_into_file(filename, list):
		with open(filename, 'w') as f:
			for s in list:
				f.write(s + '\n')
	
	def write_dict_to_file(filename, dict):
		with open(filename, 'w') as f:
			f.write(repr(dict))
	
	def clear_folder(folderName):
		files = glob.glob(folderName)
		for f in files:
			os.remove(f)
	
	def open_file(filename):
		file = open(filename, 'wb')
		return file
	
	def close_file(file):
		close(file)
		
	