# coding: utf-8
#/usr/bin/python

import sys
import os
import re

class Hadoop2_Attempts_Report:

	import shlex
	import prettytable

	_master_header = ["jobid", "jobName"]
	_job_header_read = ["finishedMaps","finishedReduces","failedMaps","failedReduces","jobStatus"]
	_task_attempt_header_read = ["taskid","attemptId","status","error","containerId"]
	_task_header_read = ["taskid","status","error"]

	_job_header = _master_header + _job_header_read
	_job_table = prettytable.PrettyTable(_job_header)
	_job_table.padding_width = 1	

	_attempts_header = _master_header + _task_attempt_header_read
	_attempts_table = prettytable.PrettyTable(_attempts_header)
	_attempts_table.padding_width = 1

	_task_header = _master_header + _task_header_read
	_task_table = prettytable.PrettyTable(_task_header)
	_task_table.padding_width = 1

	def job_print(cls, file_line):
		
		job_readings = []

		for each_item in cls._job_header:
			job_readings = job_readings + ["N/A"]


		for line in file_line:

			if '"type":"AM_STARTED"' in line or '"type":"JOB_SUBMITTED"' in line or '"type":"JOB_FINISHED"' in line or '"type":"JOB_FAILED"' in line or '"type":"JOB_KILLED"' in line:
				#print(line)

				if '"type":"AM_STARTED"' in line:
					container_line = line.replace('"', '').strip()
					
					outer_line2 = cls.outer_bracket_reg_expression(container_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line = cls.outer_bracket_reg_expression(outer_line1)
					job_readings = cls.job_readings_list(final_line, job_readings)
					#print(job_readings)

				if '"type":"JOB_SUBMITTED"' in line:
					job_line = line.replace('"', '').strip()
					
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.inner_bracket_reg_expression(outer_line1)
					job_readings = cls.job_readings_list(final_line, job_readings)
					#print(job_readings)

									
				if '"type":"JOB_FINISHED"' in line:
					
					job_line = line.replace('"', '').strip()
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.inner_bracket_reg_expression(outer_line1)
					job_readings = cls.job_readings_list(final_line, job_readings)
					#print(job_readings)


				if 	'"type":"JOB_FAILED"' in line:
					
					job_line = line.replace('"', '').strip()				
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.outer_bracket_reg_expression(outer_line1)
					job_readings = cls.job_readings_list(final_line, job_readings)
					#print(job_readings)

				if '"type":"JOB_KILLED"' in line:
					job_line = line.replace('"', '').strip()				
					outer_line2 = cls.outer_bracket_reg_expression(job_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.outer_bracket_reg_expression(outer_line1)
					job_readings = cls.job_readings_list(final_line, job_readings)
					#print(job_readings)
						
					
		return job_readings

	def job_readings_list(cls, final_line, job_readings):

		job_list = final_line.split(",")
		for each_job_item in job_list:
			(job_lb, job_rb) = each_job_item.split(":", 1)
			#print(job_lb, " = " ,job_rb)

			if job_lb in cls._job_header:
				job_table_row_index = cls._job_header.index(job_lb)
			 	#print(job_lb, " = " ,job_rb)
				job_readings[job_table_row_index] = job_rb
				#print(job_readings)

		return job_readings	

	def task_attempts_print(cls, line):

		task_attempts_list = []
		
		for each_item in cls._task_attempt_header_read:
			task_attempts_list = task_attempts_list + ["N/A"]

		if '"taskStatus":"SUCCEEDED"' not in line:
			if '"type":"MAP_ATTEMPT' in line or '"type":"REDUCE_ATTEMPT' in line:
				if 'ATTEMPT_KILLED' in line or 'ATTEMPT_FAILED' in line:
					attempt_line = line.replace('"', '').strip()
					outer_line2 = cls.outer_bracket_reg_expression(attempt_line)
					outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
					final_line	= cls.inner_bracket_reg_expression(outer_line1)
					task_attempts_list = cls.task_attempts_readings_list(final_line, task_attempts_list)	

		return task_attempts_list

	def task_attempts_readings_list(cls, final_line, task_attempts_list):

		task_list = final_line.split(",")
		for each_attempt_item in task_list:
			(attempts_lb, attempts_rb) = each_attempt_item.split(":",1)
			if attempts_lb in cls._task_attempt_header_read:
				attempts_table_row_index = cls._task_attempt_header_read.index(attempts_lb)
				#print(attempts_table_row_index)
			 	#print(attempts_lb, " = " ,attempts_rb)
				task_attempts_list[attempts_table_row_index] = attempts_rb
				#print(task_attempts_list)

		return task_attempts_list	

	def get_container_id(cls, attempts_task_table_row, file_line):

		containerId = None			
		taskid_index = cls._attempts_header.index("taskid")
		task_id = attempts_task_table_row[taskid_index]

		for line in file_line:
			if 'ATTEMPT_STARTED"' in line and task_id in line and 'startTime' in line:
				containerId = cls.get_container_id_readings(line)
					
		return containerId	

	def task_print(cls, line):
	
		failed_task_list = []
		
		for each_item in cls._task_header_read:
			failed_task_list = failed_task_list + ["N/A"]

		if '"status":"SUCCEEDED"' not in line:			
			if '"type":"TASK_FAILED"' in line or '"type":"TASK_KILLED"' in line:

				outer_line2 = cls.outer_bracket_reg_expression(line)
				outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
				final_line = cls.search_attempt_line(outer_line1,"org.apache.hadoop.mapreduce.jobhistory",'","counters')
				if '"error":""' not in final_line:				
					failed_task_list = cls.task_failed_readings_list_error_message(final_line, failed_task_list)
				else:				
					failed_task_list = cls.task_failed_readings_list_no_error_message(final_line, failed_task_list)	
		
		return failed_task_list	

	def task_failed_readings_list_error_message(cls, final_line, failed_task_list):

		final_line = final_line.replace('"','').strip()
		task_list = final_line.split(",")

		error_index = task_list.index("error:")
		error_string = task_list[error_index] + task_list[error_index + 1]
		task_list[error_index] = error_string
		task_list.pop(error_index + 1)

		failed_task_list = cls.task_failed_readings_list(final_line, failed_task_list, task_list)
		return failed_task_list

	def task_failed_readings_list_no_error_message(cls, final_line, failed_task_list):
		
		final_line = final_line.replace('"','').strip()
		task_list = final_line.split(",")
		failed_task_list = cls.task_failed_readings_list(final_line, failed_task_list, task_list)
		return failed_task_list	

	def task_failed_readings_list(cls, final_line, failed_task_list, task_list):

		# A typical taskid element in task_list looks like .TaskFailed:{taskid:task_1380254592620_0001_m_000001 , below two lines are to get task id out of it, and replace it as first element in task_list
		task_id_string = ''.join(task_list[0]) 
		task_list[0] = cls.task_id_reg_expression(task_id_string)

		if task_list:
			for each_task_item in task_list:

				(task_failed_lb, task_failed_rb) = each_task_item.split(":",1)
				#print(task_failed_lb, "=" ,task_failed_rb)
				if task_failed_lb == "error":
					error_index = task_list.index(each_task_item)
					#print(error_index)	

				if task_failed_lb in cls._task_header_read:
					task_table_row_index = cls._task_header_read.index(task_failed_lb)
					failed_task_list[task_table_row_index] = task_failed_rb

			#print(failed_task_list)		
		return failed_task_list			
			
	def get_container_id_readings(cls, line):

		containerId = None
		attempt_line = line.replace('"', '').strip()
		outer_line2 = cls.outer_bracket_reg_expression(attempt_line)
		outer_line1 = cls.outer_bracket_reg_expression(outer_line2)
		final_line = cls.outer_bracket_reg_expression(outer_line1)
		attempt_list = final_line.split(",")
		for each_attempt_item in attempt_list:
			(attempts_lb, attempts_rb) = each_attempt_item.split(":",1)

			if attempts_lb == "containerId":
				containerId = attempts_rb

		return containerId		

	def search_attempt_line(cls, line, string_left, string_right):

		final_search_string = string_left + "(.*)" + string_right
		result = re.search(final_search_string, line)
		return result.group(1)

	def outer_bracket_reg_expression(cls, line):
		outer = re.compile("\{(.+)\}")	
		line_strip_outer_bracket = outer.search(line)
		return line_strip_outer_bracket.group(1)

	def inner_bracket_reg_expression(cls, line):
		inner = re.compile("\{(.+)\{")	
		line_strip_inner_bracket = inner.search(line)
		return line_strip_inner_bracket.group(1)

	def task_id_reg_expression(cls, line):
		outer = re.compile("\{(.+)")	
		line_strip_outer_bracket = outer.search(line)
		return line_strip_outer_bracket.group(1)	

	def check_not_available_statistics(cls, task_table_row, header_line):

		check_stats_bool = True
		if "status" in header_line:
			header_status_index = header_line.index("status")
			if(task_table_row[header_status_index] == "N/A"):
				check_stats_bool = False 
			else:
				check_stats_bool = True

		return 	check_stats_bool		


	def get_parse_history_job_logs(cls, history_file_location):

		job_table_blank_row = []
	
		try:	

			with open(history_file_location, 'r') as history_file:
				file_line = history_file.readlines()

				for each_item in cls._job_header:
					job_table_blank_row = job_table_blank_row + [" "]

				#Adding row for tasks
				job_table_row = cls.job_print(file_line)
				cls._job_table.add_row(job_table_row)
				#job_table.add_row(job_table_blank_row)

				# Adding rows for attempts tasks
				for line in file_line:
					attempts_task_table_row = []
					attempts_task_table_row.insert(0,job_table_row[0])
					attempts_task_table_row.insert(1,job_table_row[1])
					attempts_task_table_row = attempts_task_table_row + cls.task_attempts_print(line)
					#print(attempts_task_table_row)
					attempts_check_stats_bool = cls.check_not_available_statistics(attempts_task_table_row, cls._attempts_header)
					if(attempts_check_stats_bool == True):
						containerId = cls.get_container_id(attempts_task_table_row, file_line)
						attempts_task_table_row.pop()
						attempts_task_table_row.append(containerId)
						cls._attempts_table.add_row(attempts_task_table_row)

				#Adding rows for tasks	
				for line in file_line:
					task_table_row = []
					task_table_row.insert(0,job_table_row[0])
					task_table_row.insert(1,job_table_row[1])
					task_table_row = task_table_row + cls.task_print(line)	
					#print(task_table_row)		
					task_check_stats_bool = cls.check_not_available_statistics(task_table_row, cls._task_header)
					if(task_check_stats_bool == True):
						cls._task_table.add_row(task_table_row)

		except IOError as err:
			print('File error' + str(err))	
			

	def print_attempts_status(cls):
	
		print("\n JOB STATISTICS - If jobStatus is N/A , that means its successful \n")
		print(cls._job_table)
		print("\n TASK ATTEMPT STATISTICS \n ")
		print(cls._attempts_table)
		print("\n TASK STATISTICS \n ")
		print(cls._task_table)		

	def getAttemptsReport(cls, history_logs_path, history_logs_type):

		try:
			for r,d,f in os.walk(history_logs_path):
				if history_logs_path:
					for files in f:
						if history_logs_type in files:
							files_path = os.path.join(r,files)
							cls.get_parse_history_job_logs(files_path)
							print("parsing done for : " + files_path)

			cls.print_attempts_status()				

		except Exception,e:
			print(e)

Hadoop2 = Hadoop2_Attempts_Report()
#if len(sys.argv) < 2:
#  print("Please enter the path where Hadoop2 history logs are located ...Exiting System")
#  sys.exit(0)
#else:
#  hadoop2_history_logs_path = sys.argv[1]
#  Hadoop2.getAttemptsReport = (hadoop2_history_logs_path,"jhist") 
  #Hadoop2.getAttemptsReport("/Users/samin/history_logs/yarn_history_logs/done/2013/09/","jhist")

Hadoop2.getAttemptsReport("/Users/samin/history_logs/final_scripts/Hadoop2_task_attempts_stats/history_logs_files/","jhist")	


 
