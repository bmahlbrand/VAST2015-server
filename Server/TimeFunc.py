from datetime import datetime

def time_func_python_date_to_solr_date(dt):
		return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def time_func_solr_date_to_python_date(str):
		return datetime.strptime(str, "%Y-%m-%dT%H:%M:%SZ")