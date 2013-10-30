#/usr/bin/python

import sys
import os
import re
import datetime
import time
import subprocess
import prettytable
import shutil

curr_script_location = os.path.abspath(__file__)
curr_script_dir = os.path.dirname(curr_script_location)
test_queries_temp_dir = os.path.join(curr_script_dir + "/test_queries_temp")
orig_queries_dir = os.path.join(curr_script_dir + "/test_queries")
init_sql_dir = os.path.join(curr_script_dir + "/init_sql_location")
master_header = "Query"

if os.path.exists(test_queries_temp_dir):
	shutil.rmtree(test_queries_temp_dir)
else:
	os.mkdir(test_queries_temp_dir)

if len(sys.argv) < 4:
	print("You have to enter No of Runs ,Name of Database and CONFIG type seperated by space ...Exiting System")
	sys.exit(0)
else:
	NO_OF_RUNS = sys.argv[1]
	NAME_OF_DATABASE = sys.argv[2]
	CHOOSE_CONFIG = sys.argv[3]
print("\n")


if(isinstance(int(NO_OF_RUNS), int) ==  True):
	print("Total Runs = " + NO_OF_RUNS)
else:
	print ("Sorry .. Enter a number as first argument which is number of rounds ... Exiting System")
	sys.exit(0)

if(isinstance(NAME_OF_DATABASE, str) == True):
	print("Name of Database = " + NAME_OF_DATABASE)
else:
	print("Sorry ... Enter a String as second argument as second argument is name of database ... Exiting System")
	sys.exit(0)

print "You choose : " + CHOOSE_CONFIG

print "\n\n"

print "Original queries are located under " + orig_queries_dir
print "All types of init.sql are located under " + init_sql_dir

def init_header_all_rounds(NO_OF_RUNS):
	all_header = []
	all_header.append(master_header)
	for i in range(1,int(NO_OF_RUNS)+1):
		all_header.append("R_" + str(i))
	return all_header

def init_header_min_rounds():
	min_header = []
	min_header.append(master_header)
	min_header.append("Minimum")
	return min_header	

def init_header_failures_rounds():
	failures_header = []
	failures_header.append(master_header)
	failures_header.append("Failures")
	return failures_header	

def create_test_queries_temp():
	
	if os.path.exists(test_queries_temp_dir):
		shutil.rmtree(test_queries_temp_dir)
	os.mkdir(test_queries_temp_dir)
	print("Creating .. " + test_queries_temp_dir)

def find_file_path_name(dest_dir, selection_criteria):
	files_path_list = []
	files_name = []
	if dest_dir:
		try:
			for r,d,f in os.walk(dest_dir):
				for files in f:
					if selection_criteria in files:
						files_path = os.path.join(r, files)
						files_path_list.append(files_path)
						files_name.append(files)
		except Exception,e:
			print ("Problem in finding files at " + dest_dir + ":" + str(e))
	else:
		print("Please enter valid destination dir ...Exiting system ")
		sys.exit(0)
	return files_path_list, files_name
	
def create_multiple_copies(orig_queries_dir, orig_query_name_list, test_queries_temp_dir):
	
	for each_query_name in orig_query_name_list:
		try:
			with open(orig_queries_dir + "/" + each_query_name, 'r') as query_file_obj:
				source_content = query_file_obj.read()
				new_query_name = each_query_name[:-4] + "-chained.sql"
				
				query_paste_count = int(NO_OF_RUNS)
				with open(test_queries_temp_dir + "/" + new_query_name,'a') as target_file_obj:
					while(query_paste_count > 0):
						target_file_obj.write(source_content)
						query_paste_count = query_paste_count - 1
			print("Done creating " + new_query_name + " inside " + test_queries_temp_dir)
		
		except Exception,e:
			print(str(e))

global time_count
time_count = 1 

def get_each_query_time(line):
	time_found = re.search('Time taken: (.+?) seconds, Fetched:',line)
	if time_found:
		return time_found.group(1)

def get_original_query_name(each_query_name):
	if each_query_name:
		return each_query_name[:-12] + ".sql"

def get_time_failures(stdoutline, all_rounds_list, failure_rounds_list, query_name):

	if "Fetched:" in stdoutline and "Time taken" in stdoutline:
		time_found = get_each_query_time(stdoutline)
		if time_found:
			global time_count
			all_rounds_list[time_count] = time_found
			time_count = time_count + 1
	if "FAILED:" in stdoutline:		
			failure_rounds_list[1] = stdoutline.replace(',','').strip("\n")

	return all_rounds_list, failure_rounds_list		
	
def run(cmd, query_name, all_rounds_list, failure_rounds_list):
	stdout = ""
	proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	while proc.poll() is None:
		stdoutline = proc.stdout.readline()
		if stdoutline:
			stdout +=stdoutline
			print(stdoutline.strip())
			query_time_list, query_timings = get_time_failures(stdoutline, all_rounds_list, failure_rounds_list, query_name)
	remaining = proc.communicate()
	remaining = remaining[0].strip()
	if remaining != "":
		stdout += remaining
		for line in remaining.split("\n"):
			print(line.strip())
			query_time_list, query_timings = get_time_failures(stdoutline, all_rounds_list, failure_rounds_list, query_name)
	
	global time_count
	time_count = 1	
	return proc.returncode, all_rounds_list, query_timings	
	#return proc.returncode

def change_string_list_int(query_time_string_list):
	if query_time_string_list:
		query_time_float_list = map(float,query_time_string_list)
		query_time_formatted_list = [ '%.2f' % elem for elem in query_time_float_list ]
	return query_time_formatted_list

def find_avg_list(query_time_list):
	if query_time_list:
		return (sum(float(each_item) for each_item in query_time_list))/len(query_time_list)

def find_min_list(query_time_list):
	if query_time_list:
		return(min(float(each_item) for each_item in query_time_list))

def remove_some_NA_from_list(query_time_list):
	if query_time_list:
		return [query_time_item for query_time_item in query_time_list if query_time_item != 'N/A']

def init_table(header):
	table_name = prettytable.PrettyTable(header)
	table_name.align["Query"] = "l"
	table_name.padding_width = 1
	return table_name

def add_row_table(table_instance, query_list):
	if query_list:
		try:
			table_instance.add_row(query_list)
		except Exception,e:
			print "Problem in add row to " ,table_instance ," : " + str(e)
			pass

create_test_queries_temp()

orig_query_path_list, orig_query_name_list = find_file_path_name(orig_queries_dir, "query")
if orig_query_name_list:
	create_multiple_copies(orig_queries_dir, orig_query_name_list, test_queries_temp_dir)
else:
	print "Problem in finding originial queries ... Exiting system"
	sys.exit(0)

init_query_path_list, init_query_name_list = find_file_path_name(init_sql_dir, CHOOSE_CONFIG) 

new_chained_query_path_list, new_chained_query_name_list = find_file_path_name(test_queries_temp_dir, "query")

print "\n\n"

if new_chained_query_name_list:	
	for each_query in new_chained_query_name_list:
		print "Looking forward to execute : " + each_query
else:
	print("Problem in finding new chained query inside " + test_queries_temp_dir + " ... Exiting System")
	sys.exit(0)

header_all_rounds = init_header_all_rounds(NO_OF_RUNS)
header_min_rounds = init_header_min_rounds()
header_failures_rounds = init_header_failures_rounds()

all_rounds = init_table(header_all_rounds)
min_rounds = init_table(header_min_rounds)
failures_rounds = init_table(header_failures_rounds)

for each_query_name in new_chained_query_name_list:
	for each_init_name in init_query_name_list:
		
		cmd = ("hive -i " + init_sql_dir + "/" + each_init_name + " -f " + test_queries_temp_dir + "/" + each_query_name + " -d DB=" + NAME_OF_DATABASE)
		print(cmd)
		
		all_rounds_list = []
		min_rounds_list = []
		failure_rounds_list = []
		
  		orig_query_name = get_original_query_name(each_query_name)

		min_rounds_list.append(orig_query_name)
		min_rounds_list.append('N/A')
		
		all_rounds_list.append(orig_query_name)
		for x in range(0,len(header_all_rounds)-1):
			all_rounds_list.append('N/A')
		
		failure_rounds_list.append(orig_query_name)
		failure_rounds_list.append('N/A')
			
		query_status, query_time_list, failed_query_list = run(cmd, orig_query_name, all_rounds_list, failure_rounds_list)
	
		if query_status == 0:
			print "Query " + orig_query_name + " executed successfully. Check out table with all timings and average for more details.\n\n"
		
		add_row_table(all_rounds, query_time_list)
		
		if 'N/A' not in query_time_list:
			min_rounds_list[1] = find_min_list(query_time_list[1:])
		else:
			new_min_list = remove_some_NA_from_list(query_time_list[1:])
			if len(new_min_list) > 0:
				min_rounds_list[1] = find_min_list(new_min_list)		

		add_row_table(min_rounds, min_rounds_list)
		add_row_table(failures_rounds, failed_query_list)

print("\n\n\n Summary of all rounds in seconds...\n")
try:
	print all_rounds.get_string(sortby="Query")
except Exception,e:
	print("Problem in printing table with all rounds : " , str(e))
	pass


print("\n\nFastest Execution Analysis\n")
try:
	print min_rounds.get_string(sortby="Query")
except Exception,e:
	print("Problem in printing average of all rounds : " , str(e))
	pass

	
print("\n\n Failed Queries \n")
try:
	print failures_rounds.get_string(sortby="Query")
except Exception,e:
	print("Problem in printing failure message of queries for all rounds : " , str(e))
	pass


