class Hadoop1PerfCounters:

	import shlex

	_master_header = ["JOBID", "JOBNAME"]
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
		
		job_parent = []
		job_parent_counters = []	
		job_bool = True
		job_table_row = []
		job_graph_readings_list = []
		TEMP_JOB_STATUS = None
		SUBMIT_TIME = None
		FINISH_TIME = None
		ELAPSED_TIME = None

		for each_item in cls._job_header:
			job_graph_readings_list.append(each_item + ":" + 'N/A')
			job_table_row = job_table_row + ['N/A']

		for line in file_line:
			if "JOBID" in line:
				job_parent = shlex.split(line)
				for job_list in job_parent:
					if '=' in job_list: 
					
						job_list = job_list.replace('"', '').strip()
						(job_lb, job_rb)= job_list.split('=',1)

						if job_lb in cls._job_header:
							job_header_item_index = cls._job_header.index(job_lb)
							
							job_table_row[job_header_item_index] = job_rb
							job_graph_readings_list[job_header_item_index] = job_lb + ":" + job_rb	
		
		job_table_row, job_graph_readings_list = cls.calculate_job_readings(job_table_row, job_graph_readings_list)				

		return job_table_row, job_graph_readings_list

	@classmethod	
	def calculate_job_readings(cls, job_readings, job_graph_readings_list):

		submit_time_index = cls._job_header_read.index("SUBMIT_TIME") + len(cls._master_header)
		finish_time_index = cls._job_header_read.index("FINISH_TIME") + len(cls._master_header)
		totalMaps_index = cls._job_header_read.index("TOTAL_MAPS") + len(cls._master_header)
		totalReduces_index = cls._job_header_read.index("TOTAL_REDUCES") + len(cls._master_header)
		elapsedTime_index = cls._job_header_read.index("ELAPSED_TIME") + len(cls._master_header)
		avg_map_time_index = cls._job_header_read.index("AVG_MAP_TIME") + len(cls._master_header)
		avg_reduce_time_index = cls._job_header_read.index("AVG_REDUCE_TIME") + len(cls._master_header)

		try:

			job_readings[elapsedTime_index] = int(job_readings[finish_time_index]) - int(job_readings[submit_time_index])
			job_graph_readings_list[elapsedTime_index] = "ELAPSED_TIME" + ":" + str(int(job_readings[finish_time_index]) - int(job_readings[submit_time_index]))
			
			if 	job_readings[totalMaps_index] != str(0):
				job_readings[avg_map_time_index] = int(job_readings[elapsedTime_index])/int(job_readings[totalMaps_index])
				job_graph_readings_list[avg_map_time_index] = "AVG_MAP_TIME" + ":" +  str(int(job_readings[elapsedTime_index])/int(job_readings[totalMaps_index]))
			else:
				job_readings[avg_map_time_index] = "AVG_MAP_TIME" + ":" + 'N/A'

			if 	job_readings[totalReduces_index] != str(0):
				job_readings[avg_reduce_time_index] = int(job_readings[elapsedTime_index]) / int(job_readings[totalReduces_index])
				job_graph_readings_list[avg_reduce_time_index] = "AVG_REDUCE_TIME" + ":" +  str(int(job_readings[elapsedTime_index]) / int(job_readings[totalReduces_index]))
			else:
				job_graph_readings_list[avg_reduce_time_index] = "AVG_REDUCE_TIME" + ":" + 'N/A'	

		except Exception,e:
			logger.info(" Occured during ELAPSED_TIME, avg map AVG_MAP_TIME or avg AVG_REDUCE_TIME calculation : " , e)
		

		return job_readings, job_graph_readings_list			

	@classmethod
	def job_get_total_counters(cls, file_line):
		
		job_parent = []
		job_bool = True
		job_table_row = []
		TEMP_JOB_STATUS = None
		map_list = []
		job_total_counters_readings = []
		job_total_map_counters_readings = []
		job_total_reduce_counters_readings = []
		job_total_counters_graph_list = []
		job_total_map_counters_graph_list = []
		job_total_reduce_counters_graph_list = []

		#Initializing the list with "N/A" and later replacing as we find the counters value
		for each_counter in cls._job_total_counters_header_read:
			job_total_counters_readings.append('N/A')
			job_total_counters_graph_list.append(each_counter + ":" + 'N/A')
			
		for each_counter in cls._job_total_map_counters_header_read:
			job_total_map_counters_readings.append('N/A')
			job_total_map_counters_graph_list.append(each_counter + ":" + 'N/A')

		for each_counter in cls._job_total_reduce_counters_header_read:
			job_total_reduce_counters_readings.append('N/A')
			job_total_reduce_counters_graph_list.append(each_counter + ":" + 'N/A')
			
		for line in file_line:
			if "JOBID" in line and "MAP_COUNTERS" in line:
				line = line.strip()
				job_parent = line.split('"')

				job_parent = filter(None, job_parent) 	
				
				job_total_counters_graph_list = cls.get_each_counters(' COUNTERS=', job_parent, cls._job_total_counters_header_read, job_total_counters_graph_list)

				job_total_map_counters_graph_list = cls.get_each_counters(' MAP_COUNTERS=', job_parent, cls._job_total_map_counters_header_read, job_total_map_counters_graph_list)
				
				job_total_reduce_counters_graph_list = cls.get_each_counters(' REDUCE_COUNTERS=', job_parent, cls._job_total_reduce_counters_header_read, job_total_reduce_counters_graph_list)
				
		return job_total_counters_graph_list, job_total_map_counters_graph_list, job_total_reduce_counters_graph_list

	@classmethod	
	def get_each_counters(cls, counter_identity, job_parent, MR_counter_header, job_counters_graph_list):

		each_counters_string_index = job_parent.index(counter_identity)
		if each_counters_string_index != " ":
				each_counters_value_index = each_counters_string_index + 1
				each_counters_string = job_parent[each_counters_value_index]
				each_MR_counter_string = re.findall(r'\[([^]]*)\]',each_counters_string)
				if len(each_MR_counter_string) > 0:
					for each_counter in each_MR_counter_string:				
						each_counter_list = re.split('[()]', each_counter)				
						each_counter_list_length = len(each_counter_list)
						#Ignoring the value at 0th position as it is non required string
						if each_counter_list[1] in MR_counter_header:
							index_counter_exists = MR_counter_header.index(each_counter_list[1])
							job_counters_graph_list[index_counter_exists] = each_counter_list[1] + ":" + str(int(each_counter_list[each_counter_list_length-2]))
							
		return job_counters_graph_list						

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
						avg_job_MR_counters_list[MR_value_index] = avg_MR_value
						avg_job_MR_graph_list[MR_value_index] = MR_counters_lb + ":" + str(avg_MR_value)
					else:
						avg_job_MR_counters_list[MR_value_index] = "N/A"
						avg_job_MR_graph_list[MR_value_index] = MR_counters_lb + ":" + 'N/A'	
				
				except Exception,e:
					logger.info("Occured during calculating average from total counters " , e)	

		return avg_job_MR_counters_list, avg_job_MR_graph_list	
	
	@classmethod	
	def get_total_maps(cls, basic_job_list):

		try:
			if basic_job_list:
				for each_item in basic_job_list:
					(readings_lb, readings_rb) = each_item.split(":",1)
					if readings_lb == "TOTAL_MAPS":
						total_maps = readings_rb						
		except Exception,e:
			logger.info("Problem in finding total Maps : " + str(e))
		
		return total_maps	

	@classmethod		
	def get_total_reduces(cls, basic_job_list):

		try:
			if basic_job_list:
				for each_item in basic_job_list:
					(readings_lb, readings_rb) = each_item.split(":",1)
					if readings_lb == "TOTAL_REDUCES":
						total_reduces = readings_rb	
		except Exception,e:
			logger.info("Problem in finding total Reduces : " + str(e))
		
		return total_reduces	
	
	@classmethod	
	def get_parse_history_job_logs(cls, history_file_location):

		try:
			
			with open(history_file_location, 'r') as history_file:
				file_line = history_file.readlines()

				job_table_row_stats, basic_job_graph_stats = cls.job_print(file_line)

				#Get the basic job statistics
				try:
					cls._job_basic_table.add_row(job_table_row_stats)
				except Exception,e:
					logger.info("Sorry was not able to print the basic job stats : " + str(e))	
				
				#Append the total counters from job counters
				tot_job_graph_counters_list, tot_job_graph_map_counters_list, tot_job_graph_reduce_counters_list = cls.job_get_total_counters(file_line)	

				#append row to print job table			
				total_job_table_row = cls.parse_total_counters_list(tot_job_graph_counters_list)
				total_job_table_row.insert(0,job_table_row_stats[0])
				total_job_table_row.insert(1,job_table_row_stats[1])
				
				try:
					cls._job_total_table.add_row(total_job_table_row)
				except Exception,e:
					logger.info("Sorry was not able to print job table statistics : " +  str(e))

				total_job_map_counters_final_list = cls.parse_total_counters_list(tot_job_graph_map_counters_list)
				total_job_map_counters_final_list.insert(0,job_table_row_stats[0])
				total_job_map_counters_final_list.insert(1,job_table_row_stats[1])
				
				#append row to create total job map table		
				try:
					cls._job_total_map_table.add_row(total_job_map_counters_final_list)
				except Exception,e:
					logger.info("Sorry was not able to print total of map statistics : " +  str(e))

				total_job_reduce_counters_final_list = cls.parse_total_counters_list(tot_job_graph_reduce_counters_list)
				if len(total_job_reduce_counters_final_list) > 0:
					total_job_reduce_counters_final_list.insert(0,job_table_row_stats[0])
					total_job_reduce_counters_final_list.insert(1,job_table_row_stats[1])	

					#append row to create total job reduce table
					try:
						cls._job_total_reduce_table.add_row(total_job_reduce_counters_final_list)
					except Exception,e:
						logger.info("Sorry was not able to print total of reduce statistics : " +  str(e))	


				#append row to create average job map table	
				avg_job_map_counters_list, avg_job_map_graph_list = cls.job_get_avg_counters(basic_job_graph_stats, tot_job_graph_map_counters_list, cls._job_total_map_counters_header_read,"map")
				avg_job_map_counters_list.insert(0,job_table_row_stats[0])
				avg_job_map_counters_list.insert(1,job_table_row_stats[1])
				try:
					cls._job_average_map_table.add_row(avg_job_map_counters_list)
				except Exception,e:
					logger.info("Sorry was not able to print average of all map statistics : " +  str(e))
					
				#append row to create average job reduce table	
				avg_job_reduce_counters_list, avg_job_reduce_graph_list = cls.job_get_avg_counters(basic_job_graph_stats, tot_job_graph_reduce_counters_list, cls._job_total_reduce_counters_header_read,"reduce")
				if len(avg_job_reduce_counters_list) > 0:
					avg_job_reduce_counters_list.insert(0,job_table_row_stats[0])
					avg_job_reduce_counters_list.insert(1,job_table_row_stats[1])
					try:
						cls._job_average_reduce_table.add_row(avg_job_reduce_counters_list)
					except Exception,e:
						logger.info("Sorry was not able to print average of all reduce statistics : " +  str(e))

				#Below counters can be used for plotting graphs	
				#print(basic_job_graph_stats)
				#print(tot_job_graph_counters_list)
				#print(tot_job_graph_map_counters_list)
				#print(tot_job_graph_reduce_counters_list)	
				#print(avg_job_map_graph_list)
				#print(avg_job_reduce_graph_list)		

		except IOError as err:
			logger.info('File error' + str(err))	

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
				logger.info("Sorry there was a problem in Initializing perf counters headings after reading from conf file : " + str(e))	

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
				logger.info("Sorry there was a problem in Initializing pretty table headings after Initializing all counter headings : " + str(e))		
				

		except Exception,e:
			logger.info("Sorry there was a problem in reading the .conf file of performance counter" + str(e))		

		try:
			for r,d,f in os.walk(history_logs_path):
			    for files in f:
			        if "default_*" in files:
			        #if "Stage" in files:
			            files_path = os.path.join(r,files)
			            cls.get_parse_history_job_logs(files_path)
			            logger.info("parsing done for : " + files_path)

			cls.print_job_stats()	           

		except Exception,e:
			logger.info("Problem in finding the files extension in given path of path don't exists: " + str(e))		            

	@classmethod
	def get_perf_counters(cls, conf_file_location):
		
		counters_heading = []
		counters = []

		try:			
			with open(conf_file_location, 'r') as conf_file:
			
				file_line = conf_file.read()
				counters_heading = file_line.split('#')
		
		except IOError as err:
			logger.info('File error' + str(err))

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
			        if  "hadoop1" in files and ".conf" in files:
			            files_path = os.path.join(r,files)
			            cls.get_perf_counters(files_path)
			            logger.info("parsing done for : " + files_path)

		except Exception,e:
			logger.info("There was an error in reading the conf file : " + str(e))				            





