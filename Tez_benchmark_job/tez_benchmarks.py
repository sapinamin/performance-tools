#/usr/bin/python

import sys
import os
import re
import subprocess
import datetime
import time
import subprocess
import prettytable
 
if len(sys.argv) < 1:
         print("You have to enter No of Runs ...Exiting System")
         sys.exit(0)
else:
         NO_OF_RUNS = sys.argv[1]
          
print("\n")
 
if(isinstance(int(NO_OF_RUNS), int) ==  True):
        print("Total Runs = " + NO_OF_RUNS)
else:
        print ("Sorry .. Enter a number as first argument which is number of rounds ... Exiting System")
        sys.exit(0)
print("\n")


#Add all the output directories are here that you want to remove before running next round
hdfs_output_dir = []
hdfs_output_dir.append('/user/root/randomWriter_20GB_100MBpermap_output')
hdfs_output_dir.append('/user/root/randomWriter_200GB_1GBpermap_output')

print "Removing existing output directory for Sort benchmark"
for each_hdfs_output_dir in hdfs_output_dir:
	cmd_remove_hdfs_output = "hadoop dfs -rmr " + each_hdfs_output_dir
	#os.system('hadoop dfs -rmr ' + each_hdfs_output_dir)
	p = subprocess.Popen(cmd_remove_hdfs_output, shell=True, stdout=subprocess.PIPE)
	for line in p.stdout.readlines():
		print(line)
	print "Removed " + each_hdfs_output_dir

def get_time(time_line):
	res = re.search(r'The job took(.+)seconds',time_line)
	return res.group(1).strip()	

#Function to run and print the output
def run(cmd):
	stdout=""
  	proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  	while proc.poll() is None:
    		stdoutline = proc.stdout.readline()
    		if stdoutline:
      			stdout += stdoutline
      			print(stdoutline.strip())
			if "The job took" in stdout:
				job_time = get_time(stdout)
  	remaining = proc.communicate()
  	remaining = remaining[0].strip()
  	if remaining != "":
    		stdout += remaining
    		for line in remaining.split("\n"):
      			print(line.strip())
	return proc.returncode, job_time
 
cmd_list = []

job_dict ={'sort_r40_randomWriter_20GB_100MBpermap':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0-SNAPSHOT.jar sort -Dmapreduce.framework.name=yarn-tez -r 40 /user/root/randomWriter_20GB_100MBpermap /user/root/randomWriter_20GB_100MBpermap_output',
	   'sort_r40_randomWriter_200GB_1GBpermap':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0-SNAPSHOT.jar sort -Dmapreduce.framework.name=yarn-tez -r 40 /user/root/randomWriter_200GB_1GBpermap /user/root/randomWriter_200GB_1GBpermap_output',
           'shuffle_r40_keepmap_100_keepred_100':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.3.0-SNAPSHOT-tests.jar loadgen -Dmapreduce.framework.name=yarn-tez -Dmapreduce.randomtextwriter.mapsperhost=12 -r 40 -keepmap 100 -keepred 100 -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text',
	   'scanInput_100_2MB_files':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.3.0-SNAPSHOT-tests.jar loadgen -Dmapreduce.framework.name=yarn-tez -r 0 -keepmap 0 -keepred 0 -indir /user/root/scanInput_100_2MB_files',
	   'scanInput_360_1MB_files':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.3.0-SNAPSHOT-tests.jar loadgen -Dmapreduce.framework.name=yarn-tez -r 0 -keepmap 0 -keepred 0 -indir /user/root/scanInput_360_1MB_files',
	   'scanInput_360_1GB_files':'hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.3.0-SNAPSHOT-tests.jar loadgen -Dmapreduce.framework.name=yarn-tez -r 0 -keepmap 0 -keepred 0 -indir /user/root/scanInput_360_1GB_files'}

print("\nExecuting following hadoop jobs\n")
for sleep_job_name, job_cmd  in job_dict.items():
	
	print sleep_job_name," :  ",job_cmd

no_of_headers = int(NO_OF_RUNS)
header = ['Hadoop job']

for i in range(1,no_of_headers + 1):
	header.append("R_"+str(i))

all_rounds = prettytable.PrettyTable(header)
all_rounds.align["Hadoop job"] = "l"
all_rounds.padding_width = 1

header_avg_analysis = ['Hadoop job', 'Average']
avg_rounds = prettytable.PrettyTable(header_avg_analysis)
avg_rounds.align["Hadoop job"] = "l"
avg_rounds.padding_width = 1

graph_job_dict = {}

for sleep_job_name, each_cmd in job_dict.items():
	each_round = []
	avg_each_round = []
	each_round.append(sleep_job_name)
	
	for y in range(0, len(header)-1):
		each_round.append('N/A')

	avg_each_round.append(sleep_job_name)
	for x in range(0, int(NO_OF_RUNS)):
		start_time = datetime.datetime.now()
		print "\nRound : " + str(x) + " for hadoop command : " + sleep_job_name + " started at : " + str(start_time)
		
		try:
			#os.system(each_cmd)
			proc_status, job_time = run(each_cmd)
		
		except Exception,e:
			print("There was an error in running benchmark commands : " + str(e))
                
		#Creating the Round header value for header parsing
		run_header_value = "R_" + str(x+1)

		#getting index of respective round header to append its value at that position in each_round list
		run_header_index = header.index(run_header_value)

                if proc_status == 0:
			print("benchmark " + sleep_job_name + " completed succesfully")
			each_round[run_header_index]= int(job_time)
		else:
			print("benchmark " + sleep_job_name + " not completed")

		end_time = datetime.datetime.now()
		print "\nRound : " + str(x) + " for hadoop command : " + sleep_job_name + " ended at " + str(end_time)
		print "Total Time Taken(including start) for Round" + str(x) + " for hadoop command -> " + sleep_job_name + " : " + str(end_time - start_time) + "  hh:mm:ssss(format)." 	
			
	try:
		all_rounds.add_row(each_round)
	except Exception,e:
		print("There was an error in adding row to each_round list (for displaying timings of each round) : " + str(e))
		pass

	#Removing all 'N/A' to get average value for only ints in each_round list
	if 'N/A' in each_round:
		each_round.remove('N/A')

	try:
		if int(NO_OF_RUNS) > 1:	
			avg_time_each_job = sum(each_round[2:])/(len(each_round)-2)
			avg_each_round.append(avg_time_each_job)
			graph_job_dict[sleep_job_name] = avg_time_each_job
		else:
			avg_each_round.append(each_round[1])
			graph_job_dict[sleep_job_name] = (each_round[1])
	except Exception,e:
		print("There was an error in computing average of timings : " + str(e))
	
	try:
		avg_rounds.add_row(avg_each_round)
	except Exception,e:
		print("There was an error in adding row to avg_rounds list (for displaying average of all timings) : " + str(e))
		pass

print("\n\n\n Summary of all rounds in seconds...\n")

try:
	print all_rounds	
except Exception,e:
	print "Sorry cannot print summary of all rounds ... it appears that some values were missing" + str(e)

print("\n Printing average of each run : Ignore first run's result and taking average of the rest \n")

try:
        print avg_rounds        
except Exception,e:
        print "Sorry cannot print average of all rounds ...it appears that some values were missing" + str(e)

#Creating graph
for sleep_job_name, avg_time in graph_job_dict.items():

	try:
		file_name = (os.getcwd() + "/graph_plot/" + sleep_job_name)
		with open(file_name + ".plot", 'w+') as file:
			file.write("YVALUE=" + str(avg_time))		
	except IOError as err:
		print("Error occured while opening plot file" + str(err))

