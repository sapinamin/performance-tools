# coding: utf-8
#/usr/bin/python

import sys
import nester
import os
import re
import csv
import glob

class Hadoop2PerfCounters:

	import shlex

	_master_header = ["jobid", "jobName"]
	_job_header_read = []
	_job_total_counters_header_read = []
	_job_total_map_counters_header_read = []
	_job_total_reduce_counters_header_read = []

	_job_total_counters_header = []
	_job_total_map_counters_header = []
	_job_total_reduce_counter_header = []
	_job_average_map_counters_header = []
	_job_average_reduce_counters_header = []

	@classmethod	
	def job_print(cls, file_line):
		
		job_readings = []
		job_graph_readings_list = []

		for each_item in cls._job_header:
			job_readings = job_readings + ["N/A"]
			job_graph_readings_list.append(each_item + ":" + 'N/A')

		for line in file_line:

			if '"type":"JOB_SUBMITTED"' in line or '"type":"JOB_INITED"' in line or '"type":"JOB_FINISHED"' in line or '"type":"JOB_FAILED"' in line:

				if '"type":"JOB_SUBMITTED"' in line:
					job_line = line.replace('"', '').strip()
					
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.inner_bracket_reg_expression(outer_line1)
					job_readings, job_graph_readings_list = cls.job_readings_list(final_line, job_readings, job_graph_readings_list)
					

				if '"type":"JOB_INITED"' in line:
					job_line = line.replace('"', '').strip()

					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.outer_bracket_reg_expression(outer_line1)
					job_readings, job_graph_readings_list = cls.job_readings_list(final_line, job_readings, job_graph_readings_list)					
									
				if '"type":"JOB_FINISHED"' in line:

					job_line = line.replace('"', '').strip()
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.inner_bracket_reg_expression(outer_line1)
					job_readings, job_graph_readings_list = cls.job_readings_list(final_line, job_readings, job_graph_readings_list)				

				if 	'"type":"JOB_FAILED"' in line:
					
					job_line = line.replace('"', '').strip()				
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.outer_bracket_reg_expression(outer_line1)
					job_readings, job_graph_readings_list = cls.job_readings_list(final_line, job_readings, job_graph_readings_list)
						
		job_readings, job_graph_readings_list = cls.calculate_job_readings(job_readings, job_graph_readings_list)

		return job_readings, job_graph_readings_list

	@classmethod	
	def job_readings_list(cls, final_line, job_readings, job_graph_readings_list):

		job_list = final_line.split(",")
		for each_job_item in job_list:
			(job_lb, job_rb) = each_job_item.split(":", 1)

			if job_lb in cls._job_header:
				job_table_row_index = cls._job_header.index(job_lb)

				job_readings[job_table_row_index] = job_rb
				job_graph_readings_list[job_table_row_index] = job_lb + ":" + job_rb
		
		return job_readings, job_graph_readings_list	

	@classmethod	
	def calculate_job_readings(cls, job_readings, job_graph_readings_list):

		submit_time_index = cls._job_header_read.index("submitTime") + len(cls._master_header)
		finish_time_index = cls._job_header_read.index("finishTime") + len(cls._master_header)
		totalMaps_index = cls._job_header_read.index("totalMaps") + len(cls._master_header)
		totalReduces_index = cls._job_header_read.index("totalReduces") + len(cls._master_header)
		elapsedTime_index = cls._job_header_read.index("elapsedTime") + len(cls._master_header)
		avg_map_time_index = cls._job_header_read.index("avg_map_time") + len(cls._master_header)
		avg_reduce_time_index = cls._job_header_read.index("avg_reduce_time") + len(cls._master_header)

		try:

			job_readings[elapsedTime_index] = int(job_readings[finish_time_index]) - int(job_readings[submit_time_index])
			job_graph_readings_list[elapsedTime_index] = "elapsedTime" + ":" + str(int(job_readings[finish_time_index]) - int(job_readings[submit_time_index]))
			
			if 	job_readings[totalMaps_index] != str(0):
				job_readings[avg_map_time_index] = int(job_readings[elapsedTime_index])/int(job_readings[totalMaps_index])
				job_graph_readings_list[avg_map_time_index] = "avg_map_time" + ":" +  str(int(job_readings[elapsedTime_index])/int(job_readings[totalMaps_index]))
			else:
				job_readings[avg_map_time_index] = "avg_map_time" + ":" + 'N/A'

			if 	job_readings[totalReduces_index] != str(0):
				job_readings[avg_reduce_time_index] = int(job_readings[elapsedTime_index]) / int(job_readings[totalReduces_index])
				job_graph_readings_list[avg_reduce_time_index] = "avg_reduce_time" + ":" +  str(int(job_readings[elapsedTime_index]) / int(job_readings[totalReduces_index]))
			else:
				job_graph_readings_list[avg_reduce_time_index] = "avg_reduce_time" + ":" + 'N/A'	

		except Exception,e:
			print(" Occured during elapsed time, avg map time or avg reduce time : " , e)
		

		return job_readings, job_graph_readings_list

	@classmethod	
	def job_get_total_counters(cls, file_line):

		total_job_counters_list = []
		total_job_map_counters_list = []
		total_job_reduce_counters_list = []

		for each_counter in cls._job_total_counters_header_read:
			total_job_counters_list.append(each_counter + ":" + 'N/A')

		for each_counter in cls._job_total_map_counters_header_read:
			total_job_map_counters_list.append(each_counter + ":" + 'N/A')	

		for each_counter in cls._job_total_reduce_counters_header_read:
			total_job_reduce_counters_list.append(each_counter + ":" + 'N/A')	

		for line in file_line:
			if '"type":"JOB_FINISHED"' in line:

				outer_line3 = cls.outer_bracket_reg_expression(line)
				outer_line2 = cls.outer_bracket_reg_expression(outer_line3)
				outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
				final_line	= cls.outer_bracket_reg_expression(outer_line1)
				
				counters_line = final_line.split('}]},"')
				
				total_counters = counters_line[0].replace('"', '').strip()
				total_map_counters = counters_line[1].replace('"', '').strip()
				total_reduce_counters = counters_line[2].replace('"', '').strip()

				#print("\n total_counters \n")
				total_job_counters_list = cls.get_job_total_counters_readings(total_counters, cls._job_total_counters_header_read, total_job_counters_list)
		
				#print("\n tot_map_counters \n")
				total_job_map_counters_list = cls.get_job_total_counters_readings(total_map_counters, cls._job_total_map_counters_header_read, total_job_map_counters_list)

				#print("\n tot_reduce_counters \n")
				total_job_reduce_counters_list = cls.get_job_total_counters_readings(total_reduce_counters, cls._job_total_reduce_counters_header_read, total_job_reduce_counters_list)	

		return 	total_job_counters_list, total_job_map_counters_list, total_job_reduce_counters_list

	@classmethod	
	def get_job_total_counters_readings(cls, total_counters, header_read, counters_list):
		
		tot_sub1_counters = cls.get_job_total_counters(total_counters)
		tot_sub_counters = cls.remove_first_item(tot_sub1_counters)

		for each_item_sub_counters_temp in tot_sub_counters:
			split_by_counts = each_item_sub_counters_temp.split("counts:")
			split_by_counts_list = cls.remove_first_item(split_by_counts)
			#Map counters has an extra added value which is useless so we use first value in the list
			job_tot_counters_list = cls.outer_bracket_tot_reg_expression(split_by_counts_list[0])
			job_tot_counters_new_list = cls.outer_bracket_reg_expression(job_tot_counters_list).replace('},{','/')
			job_tot_counters_combine_list = job_tot_counters_new_list.split("/")
			for each_tot_counters_list_item in job_tot_counters_combine_list:
				
				job_tot_counters_final_list = each_tot_counters_list_item.split(",")
				#returns three items, we don't want the middle one
				del job_tot_counters_final_list[1]
				#strip ('name:') and ('value:') from each item
				#print(job_tot_counters_final_list[0][5:] ," = " , job_tot_counters_final_list[1][6:])
				header_lb = job_tot_counters_final_list[0][5:]
				#for each_header_item in header_read:
				#	if each_header_item == header_lb:
				
				if header_lb in header_read:	
					header_index = header_read.index(header_lb)
					counters_list[header_index] = header_lb + ":" + (job_tot_counters_final_list[1][6:].strip("}]"))
					#print(counters_list[header_index])				
					
		return counters_list				

	
	@classmethod
	def get_job_total_counters(cls, job_tot_counter):
		job_total_counters = job_tot_counter.split("name:org.apache.hadoop.mapreduce")
		return job_total_counters

	@classmethod	
	def outer_bracket_reg_expression(cls, line):
		outer = re.compile("\{(.+)\}")	
		line_strip_outer_bracket = outer.search(line)
		return line_strip_outer_bracket.group(1)

	@classmethod	
	def inner_bracket_reg_expression(cls, line):
		inner = re.compile("\{(.+)\{")	
		line_strip_inner_bracket = inner.search(line)
		return line_strip_inner_bracket.group(1)

	@classmethod	
	def outer_bracket_tot_reg_expression(cls, line):	
		inner = re.compile("\[(.*)\]")	
		line_strip_inner_bracket = inner.search(line)
		return line_strip_inner_bracket.group(1)

	@classmethod	
	def remove_first_item(cls, tot_list):
		total_list = tot_list[1:]
		return total_list

	@classmethod	
	def parse_total_counters_list(cls, counters_graph_plot_list):
		counters_list=[]
		for each_counter in counters_graph_plot_list:
			(each_counter_lb, each_counter_rb) = each_counter.split(":")
			if each_counter_rb == 'N/A':
				counters_list.append('N/A')
			else:
				counters_list.append(each_counter_rb)

		return counters_list

	@classmethod	
	def job_get_avg_counters(cls, basic_job_list, tot_MR_list, MR_header_read, MR):

		avg_job_MR_counters_list = []
		avg_job_MR_graph_list = []

		for each_counter in MR_header_read:
			avg_job_MR_counters_list.append(each_counter + ":" + 'N/A')
			avg_job_MR_graph_list.append(each_counter + ":" + 'N/A')	

		if MR == "map":	
			total_maps = cls.get_total_maps(basic_job_list)
			avg_job_MR_counters_list, avg_job_MR_graph_list = cls.get_avg_readings_list(tot_MR_list, total_maps, avg_job_MR_counters_list, avg_job_MR_graph_list, MR_header_read)	

		if MR == "reduce":
			total_reduces = cls.get_total_reduces(basic_job_list)
			avg_job_MR_counters_list, avg_job_MR_graph_list = cls.get_avg_readings_list(tot_MR_list, total_reduces, avg_job_MR_counters_list, avg_job_MR_graph_list, MR_header_read)		

		return avg_job_MR_counters_list, avg_job_MR_graph_list

	@classmethod	
	def get_avg_readings_list(cls, tot_MR_list, total_MR, avg_job_MR_counters_list, avg_job_MR_graph_list, MR_header_read):
		
		for each_item in tot_MR_list:
			(MR_counters_lb, MR_counters_rb) = each_item.split(":")
						
			if MR_counters_lb in MR_header_read:
				MR_value_index = MR_header_read.index(MR_counters_lb)
				try:
					if MR_counters_rb != "N/A":					
						avg_MR_value = int(MR_counters_rb)/int(total_MR)					
						#print(MR_counters_lb, " = " , MR_counters_rb)				
						avg_job_MR_counters_list[MR_value_index] = avg_MR_value
						avg_job_MR_graph_list[MR_value_index] = MR_counters_lb + ":" + str(avg_MR_value)
					else:
						#print(MR_counters_lb, " = " , MR_counters_rb)
						avg_job_MR_counters_list[MR_value_index] = "N/A"
						avg_job_MR_graph_list[MR_value_index] = MR_counters_lb + ":" + 'N/A'	
				
				except Exception,e:
					print("Occured during calculating average from total counters " , e)	

		return avg_job_MR_counters_list, avg_job_MR_graph_list	

	@classmethod	
	def get_total_maps(cls, basic_job_list):

		try:
			if basic_job_list:
				for each_item in basic_job_list:
					(readings_lb, readings_rb) = each_item.split(":")
					if readings_lb == "totalMaps":
						total_maps = readings_rb						
		except Exception,e:
			print (e)
		
		return total_maps	

	@classmethod	
	def get_total_reduces(cls, basic_job_list):

		try:
			if basic_job_list:
				for each_item in basic_job_list:
					(readings_lb, readings_rb) = each_item.split(":")
					if readings_lb == "totalReduces":
						total_reduces = readings_rb	
		except Exception,e:
			print (e)
		
		return total_reduces	

	@classmethod	
	def get_parse_history_job_logs(cls, history_file_location):

		try:	

			with open(history_file_location, 'r') as history_file:
				file_line = history_file.readlines()

				job_table_blank_row = []

				for each_item in cls._job_header:
					job_table_blank_row = job_table_blank_row + [" "]				

				job_table_row_stats, basic_job_graph_stats = cls.job_print(file_line)	
				
				#Get the basic job statistics
				try:
					cls._job_basic_table.add_row(job_table_row_stats)
				except Exception,e:
					print("Sorry was not able to print the basic job stats : " + str(e))					

				tot_job_graph_counters_list, tot_job_graph_map_counters_list, tot_job_graph_reduce_counters_list = cls.job_get_total_counters(file_line)

				#append row to print job table			
				total_job_table_row = cls.parse_total_counters_list(tot_job_graph_counters_list)
				total_job_table_row.insert(0,job_table_row_stats[0])
				total_job_table_row.insert(1,job_table_row_stats[1])

				try:
					cls._job_total_table.add_row(total_job_table_row)
				except Exception,e:
					print("Sorry was not able to print job table statistics : " +  str(e))	
				#job_table.add_row(job_table_blank_row)

				total_job_map_counters_final_list = cls.parse_total_counters_list(tot_job_graph_map_counters_list)
				total_job_map_counters_final_list.insert(0,job_table_row_stats[0])
				total_job_map_counters_final_list.insert(1,job_table_row_stats[1])
				try:
					cls._job_total_map_table.add_row(total_job_map_counters_final_list)
				except Exception,e:
					print("Sorry was not able to print total of map statistics : " +  str(e))
				
				total_job_reduce_counters_final_list = cls.parse_total_counters_list(tot_job_graph_reduce_counters_list)
				if len(total_job_reduce_counters_final_list) > 0:
					total_job_reduce_counters_final_list.insert(0,job_table_row_stats[0])
					total_job_reduce_counters_final_list.insert(1,job_table_row_stats[1])
					try:
						cls._job_total_reduce_table.add_row(total_job_reduce_counters_final_list)
					except Exception,e:
						print("Sorry was not able to print total of reduce statistics : " +  str(e))


				avg_job_map_counters_list, avg_job_map_graph_list = cls.job_get_avg_counters(basic_job_graph_stats, tot_job_graph_map_counters_list, cls._job_total_map_counters_header_read,"map")
				avg_job_map_counters_list.insert(0,job_table_row_stats[0])
				avg_job_map_counters_list.insert(1,job_table_row_stats[1])
				try:
					cls._job_average_map_table.add_row(avg_job_map_counters_list)
				except Exception,e:
					print("Sorry was not able to print average of all map statistics : " +  str(e))

				avg_job_reduce_counters_list, avg_job_reduce_graph_list = cls.job_get_avg_counters(basic_job_graph_stats, tot_job_graph_reduce_counters_list, cls._job_total_reduce_counters_header_read,"reduce")
				if len(avg_job_reduce_counters_list) > 0:
					avg_job_reduce_counters_list.insert(0,job_table_row_stats[0])
					avg_job_reduce_counters_list.insert(1,job_table_row_stats[1])
					try:
						cls._job_average_reduce_table.add_row(avg_job_reduce_counters_list)
					except Exception,e:
						print("Sorry was not able to print average of all reduce statistics : " +  str(e))	

				#Below counters can be used for plotting graphs
				#print(basic_job_graph_stats)
				#print(tot_job_graph_counters_list)
				#print(tot_job_graph_map_counters_list)
				#print(tot_job_graph_reduce_counters_list)	
				#print(avg_job_map_graph_list)
				#print(avg_job_reduce_graph_list)
			

		except IOError as err:
			print('File error' + str(err))	

	@classmethod		
	def print_job_stats(cls):

		print("\n BASIC JOB STATISTICS \n")
		print(cls._job_basic_table)
		print("\n TOTAL JOB STATISTICS \n")
		print(cls._job_total_table)
		print("\n TOTAL MAP CONSUMPTION STATISTICS \n")
		print(cls._job_total_map_table)
		print("\n TOTAL REDUCE CONSUMPTION STATISTICS \n")
		print(cls._job_total_reduce_table)
		print("\n AVERAGE MAP CONSUMPTION STATISTICS \n")
		print(cls._job_average_map_table)
		print("\n AVERAGE REDUCE CONSUMPTION STATISTICS \n")
		print(cls._job_average_reduce_table)				

	@classmethod
	def getPerfCountersAndReport(cls, history_logs_path, conf_file_path):

		try:
			
			cls.readPerfCountersFromConf(conf_file_path)
			
			try:	

				cls._job_total_counters_header = [ "TOT_" + each_counter for each_counter in (cls._job_total_counters_header_read)]
				
				cls._job_total_map_counters_header = [ "TOT_MAP_" + each_counter for each_counter in cls._job_total_map_counters_header_read]
				cls._job_total_map_counters_header = cls._master_header + cls._job_total_map_counters_header
				
				cls._job_total_reduce_counter_header = [ "TOT_REDUCE_" + each_counter for each_counter in cls._job_total_reduce_counters_header_read]	
				cls._job_total_reduce_counter_header = cls._master_header + cls._job_total_reduce_counter_header
					
				cls._job_average_map_counters_header = [ "AVG_MAP_" + each_counter for each_counter in cls._job_total_map_counters_header_read]			
				cls._job_average_map_counters_header = cls._master_header + cls._job_average_map_counters_header

				cls._job_average_reduce_counters_header = [ "AVG_REDUCE_" + each_counter for each_counter in cls._job_total_reduce_counters_header_read]			
				cls._job_average_reduce_counters_header = cls._master_header + cls._job_average_reduce_counters_header

			except Exception,e:
				print("Sorry there was a problem in Initializing perf counters headings after reading from conf file : " + str(e))	

			try:

				import prettytable

				cls._job_header = cls._master_header + cls._job_header_read
				cls._job_basic_table = prettytable.PrettyTable(cls._job_header)
				cls._job_basic_table.padding_width = 1

				
				cls._job_total_header = cls._master_header + cls._job_total_counters_header
				cls._job_total_table = prettytable.PrettyTable(cls._job_total_header)
				cls._job_total_table.padding_width = 1

				cls._job_total_map_table = prettytable.PrettyTable(cls._job_total_map_counters_header)
				cls._job_total_map_table.padding_width = 1

				cls._job_total_reduce_table = prettytable.PrettyTable(cls._job_total_reduce_counter_header)
				cls._job_total_reduce_table.padding_width = 1

				cls._job_average_map_table = prettytable.PrettyTable(cls._job_average_map_counters_header)
				cls._job_average_map_table.padding_width = 1

				cls._job_average_reduce_table = prettytable.PrettyTable(cls._job_average_reduce_counters_header)
				cls._job_average_reduce_table.padding_width = 1

			except Exception,e:
				print("Sorry there was a problem in Initializing pretty table headings after Initializing all counter headings : " + str(e))		
				

		except Exception,e:
			print("Sorry there was a problem in reading the .conf file of performance counter" + str(e))	
		
		try:
			for r,d,f in os.walk(history_logs_path):
				    for files in f:
				        if files.endswith("jhist"):
				           	files_path = os.path.join(r,files)
				           	cls.get_parse_history_job_logs(files_path)
					        print("parsing done for : " + files_path)
					
			cls.print_job_stats()

		except Exception,e:
			print (e)		            
	
	@classmethod
	def get_perf_counters(cls, conf_file_location):
		
		counters_heading = []
		counters = []

		try:			
			with open(conf_file_location, 'r') as conf_file:
			
				file_line = conf_file.read()
				counters_heading = file_line.split('#')
		
		except IOError as err:
			print('File error' + str(err))

		for each_counter_list in counters_heading:
			if len(each_counter_list) > 1:
				counters = each_counter_list.split('\n')		
				if counters[0] == "Basic Job Counters":
					cls._job_header_read = cls.initialize_each_counter_heading(counters[0], counters)
				if counters[0] == "Total Job Counters":
					cls._job_total_counters_header_read = cls.initialize_each_counter_heading(counters[0], counters)
				if counters[0] == "Total Map Counters":
					cls._job_total_map_counters_header_read = cls.initialize_each_counter_heading(counters[0], counters)
				if counters[0] == "Total Reduce Counters":
					cls._job_total_reduce_counters_header_read = cls.initialize_each_counter_heading(counters[0], counters)	
				
	@classmethod	
	def initialize_each_counter_heading(cls, counter_heading, counters):

		counter_final_list = []
		if counters[0] == counter_heading:
			for each_counters  in counters[1:]:
				each_final_counters = each_counters.strip()
				if each_final_counters != '':
					if '--' not in each_final_counters:
						counter_final_list.append(each_final_counters.strip())

		return counter_final_list		

	@classmethod
	def readPerfCountersFromConf(cls, conf_file_path):		

		try:
			for r,d,f in os.walk(conf_file_path):
			    for files in f:
			        if "hadoop2" in files and ".conf" in files:
			            files_path = os.path.join(r,files)
			            cls.get_perf_counters(files_path)
			            print("parsing done for : " + files_path)

		except Exception,e:
			print (e)

Hadoop2 = Hadoop2PerfCounters()
if len(sys.argv) < 3:
  print("Please enter the path where Hadoop2 history logs are located ...Exiting System")
  sys.exit(0)
else:
  hadoop2_history_logs_path = sys.argv[1]
  hadoop2_perf_conf_path = sys.argv[2]
  Hadoop2.getPerfCountersAndReport(hadoop2_history_logs_path, hadoop2_perf_conf_path)
 
#Hadoop2.getPerfCountersAndReport("/Users/samin/history_logs/yarn_history_logs/done/2013/09/", "/Users/samin/history_logs/final_scripts/")

