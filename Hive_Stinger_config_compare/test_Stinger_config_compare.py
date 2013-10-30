# coding: utf-8
#/usr/bin/python

import sys
import os
import re
import datetime
import time
import subprocess
import prettytable
import shutil
import csv

curr_script_location = os.path.abspath(__file__)
curr_script_dir = os.path.dirname(curr_script_location)
test_queries_temp_dir = os.path.join(curr_script_dir + "/test_queries_temp")
orig_queries_dir = os.path.join(curr_script_dir + "/test_queries")
init_sql_dir = os.path.join(curr_script_dir + "/init_sql_location")
master_header = "Query"
input_query_list = []
configs_list = []

if os.path.exists(test_queries_temp_dir):
	shutil.rmtree(test_queries_temp_dir)
else:
	os.mkdir(test_queries_temp_dir)

if len(sys.argv) < 5:
	print("You have to enter No of Runs ,Name of Database, types of configs(each separeated by comma) and different queries(each separated by comma) seperated by space ...Exiting System")
	sys.exit(0)
else:
	NO_OF_RUNS = sys.argv[1]
	NAME_OF_DATABASE = sys.argv[2]
	CONFIG_STRINGS = sys.argv[3]
	CHOOSE_INPUT_QUERY = sys.argv[4]
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

print "\n"

if CONFIG_STRINGS:
	configs_list = CONFIG_STRINGS.split(",")
	for each_config in configs_list:
		print "You choose config : " , each_config

if CHOOSE_INPUT_QUERY:
	input_query_list = CHOOSE_INPUT_QUERY.split(",")
	if "All" in input_query_list or "ALL" in input_query_list or "all" in input_query_list:
		print "You choose All queries"		
	else:
		for each_input_query in input_query_list:
			print "You choose : " , each_input_query , " query"

print "\n\n"

print "Original queries are located under " + orig_queries_dir
print "All types of init.sql are located under " + init_sql_dir

def init_header_all_rounds(NO_OF_RUNS, each_init_query_name):
	all_header = []
	all_header.append(master_header)
	for i in range(1,int(NO_OF_RUNS)+1):
		all_header.append("R_" + str(i))
	return all_header

def init_header_min_rounds(init_query_name_list):
	min_header = []
	min_header.append(master_header)
	for each_init in init_query_name_list:
		min_header.append(each_init)
	return min_header	

def init_header_failures_rounds(each_init_query_name):
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
				for each_criteria in selection_criteria:
					for files in f:
						if each_criteria in files:
							files_path = os.path.join(r, files)
							files_path_list.append(files_path)						
							files_name.append(files)
					
		except Exception,e:
			print ("Problem in finding files at " + dest_dir + ":" + str(e))
	else:
		print("Please enter valid directory to find files ...Exiting system ")
		sys.exit(0)
	return files_path_list, files_name
	
def create_multiple_copies(orig_queries_dir, orig_query_name_list, test_queries_temp_dir, no_of_runs):
	
	for each_query_name in orig_query_name_list:
		try:
			with open(orig_queries_dir + "/" + each_query_name, 'r') as query_file_obj:
				source_content = query_file_obj.read()
				new_query_name = each_query_name[:-4] + "-chained.sql"
				
				query_paste_count = int(no_of_runs)
				with open(test_queries_temp_dir + "/" + new_query_name,'a') as target_file_obj:
					while(query_paste_count > 0):
						target_file_obj.write(source_content)
						query_paste_count = query_paste_count - 1
			print("Done creating " + new_query_name + " inside " + test_queries_temp_dir)
		
		except Exception,e:
			print(str(e))

def choose_multiple_copies(orig_queries_dir, query_name_list, test_queries_temp_dir, NO_OF_RUNS):
	
	if orig_query_name_list:
		create_multiple_copies(orig_queries_dir, query_name_list, test_queries_temp_dir, NO_OF_RUNS)
	else:
		print "Problem in finding originial queries ... Exiting system"
		sys.exit(0)

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

def create_csv(current_list, csv_name):
	csv_file_name = test_queries_temp_dir + "/" + csv_name + ".csv"
	if current_list:
		with open(csv_file_name,"w+") as output:
			writer = csv.writer(output, lineterminator='\n')
			writer.writerow(current_list)			

def write_csv(current_list, csv_name):
	csv_file_name = test_queries_temp_dir + "/" + csv_name + ".csv"
	if current_list:
		with open(csv_file_name,"a") as output:
			writer = csv.writer(output, lineterminator='\n')
			writer.writerow(current_list)

def add_to_min_list(query_time_list, header_min_rounds, improved_min_rounds_list, init_query_name):
	if query_time_list:
		if init_query_name in header_min_rounds:
			init_query_index = header_min_rounds.index(init_query_name)
			if 'N/A' not in query_time_list:
				improved_min_rounds_list[init_query_index] = find_min_list(query_time_list[1:])
			else:
				new_min_list = remove_some_NA_from_list(query_time_list[1:])
				if len(new_min_list) > 0:
					improved_min_rounds_list[init_query_index] = find_min_list(new_min_list)
	return improved_min_rounds_list

def read_print_csv(read_csv_list_criteria):
	csv_file_path_list, csv_file_name_list = find_file_path_name(test_queries_temp_dir, read_csv_list_criteria)
	if csv_file_name_list:
		from prettytable import from_csv
		for each_csv_file in csv_file_name_list:
			file_csv = open(test_queries_temp_dir + "/" + each_csv_file , "r")
			csv_table = from_csv(file_csv)
			if "all_rounds" in each_csv_file:
				print("\n\nSummary of all rounds for " + each_csv_file[:-4])
				print(csv_table.get_string(sortby=master_header))
			if "failures" in each_csv_file:
				print("\n\n Failed Queries for " + each_csv_file[:-4])
				print(csv_table.get_string(sortby=master_header))
			if "min_comparison" in each_csv_file:
				print("\n\n Fastest Execution Analysis \n")
				print(csv_table.get_string(sortby=master_header))
			file_csv.close()


create_test_queries_temp()
chained_query_list = ["query"]

orig_query_path_list, orig_query_name_list = find_file_path_name(orig_queries_dir, chained_query_list)

if "All" in input_query_list or "all" in input_query_list or "ALL" in input_query_list:
	choose_multiple_copies(orig_queries_dir, orig_query_name_list, test_queries_temp_dir, NO_OF_RUNS)
else:
	choose_multiple_copies(orig_queries_dir, input_query_list, test_queries_temp_dir, NO_OF_RUNS)
		
init_query_path_list, init_query_name_list = find_file_path_name(init_sql_dir, configs_list)
new_chained_query_path_list, new_chained_query_name_list = find_file_path_name(test_queries_temp_dir, chained_query_list)

print "\n\n"

if new_chained_query_name_list:	
	for each_query in new_chained_query_name_list:
		print "Looking forward to execute : " + each_query
else:
	print("Problem in finding new chained query inside " + test_queries_temp_dir + " ... Exiting System")
	sys.exit(0)

for each_init_query_name in init_query_name_list:

	header_all_rounds = init_header_all_rounds(NO_OF_RUNS, each_init_query_name)
	csv_all_rounds_name = each_init_query_name[:-4] + "_all_rounds"
	create_csv(header_all_rounds, csv_all_rounds_name)

	header_failures_rounds = init_header_failures_rounds(each_init_query_name)
	csv_failures_rounds_name = each_init_query_name[:-4] + "_failures_rounds"
	create_csv(header_failures_rounds,csv_failures_rounds_name)	

header_min_rounds = init_header_min_rounds(init_query_name_list)
create_csv(header_min_rounds, "init_min_comparison")

print(header_min_rounds)

for each_query_name in new_chained_query_name_list:

	min_rounds_list = []
	for each_header_item in header_min_rounds:
		min_rounds_list.append('N/A')

	for each_init_name in init_query_name_list:
	
		cmd = ("hive -i " + init_sql_dir + "/" + each_init_name + " -f " + test_queries_temp_dir + "/" + each_query_name + " -d DB=" + NAME_OF_DATABASE)
		print(cmd)
	
		all_rounds_list = []
		failure_rounds_list = []
				
		orig_query_name = get_original_query_name(each_query_name)

		min_rounds_list[0] = orig_query_name

		all_rounds_list.append(orig_query_name)
		for x in range(1,int(NO_OF_RUNS)+1):
			all_rounds_list.append('N/A')
		
		failure_rounds_list.append(orig_query_name)
		failure_rounds_list.append('N/A')
			
		query_status, query_time_list, failed_query_list = run(cmd, orig_query_name, all_rounds_list, failure_rounds_list)
		
		if query_status == 0:
			print "Query " + orig_query_name + " executed successfully. Check out table with all timings and fastest execution analysis for more details.\n\n"
		
		write_csv(all_rounds_list, each_init_name[:-4] + "_all_rounds")	
		write_csv(failed_query_list, each_init_name[:-4] + "_failures_rounds")
		
		min_rounds_list = add_to_min_list(query_time_list, header_min_rounds, min_rounds_list, each_init_name)
	
	write_csv(min_rounds_list,"init_min_comparison")

read_all_rounds_csv = ["all_rounds.csv"]
read_failures_rounds_csv = ["failures_rounds.csv"]
read_min_rounds_csv = ["init_min_comparison.csv"]

try:
	read_print_csv(read_all_rounds_csv)
except Exception,e:
	print("Problem in printing table with all rounds : " , str(e))
	pass

try:
	read_print_csv(read_failures_rounds_csv)
except Exception,e:
	print("Problem in printing failure message of queries for all rounds : " , str(e))
	pass

try:
	read_print_csv(read_min_rounds_csv)
except Exception,e:
	print("Problem in printing average of all rounds : " , str(e))
	pass


