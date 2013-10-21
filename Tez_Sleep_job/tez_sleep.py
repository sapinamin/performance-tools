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

cmd_list = []

job_dict = {'sleep_m1_r0':'$HADOOP_PREFIX/bin/hadoop jar /grid/0/tez/lib/tez-mapreduce-examples-0.2.0-SNAPSHOT.jar mrrsleep -m 1 -r 0 -ir 0 -irs 0 -mt 1',
	    'sleep_m100_r0':'$HADOOP_PREFIX/bin/hadoop jar /grid/0/tez/lib/tez-mapreduce-examples-0.2.0-SNAPSHOT.jar mrrsleep -m 100 -r 0 -ir 0 -irs 0 -mt 1',
	    'sleep_m1_r1':'$HADOOP_PREFIX/bin/hadoop jar /grid/0/tez/lib/tez-mapreduce-examples-0.2.0-SNAPSHOT.jar mrrsleep -m 1 -r 1 -ir 0 -irs 0 -mt 1 -rt 1',
	    'sleep_m100_r1':'$HADOOP_PREFIX/bin/hadoop jar /grid/0/tez/lib/tez-mapreduce-examples-0.2.0-SNAPSHOT.jar mrrsleep -m 100 -r 1 -ir 0 -irs 0 -mt 1 -rt 1'}

print("Executing following hadoop jobs")
for sleep_job_name, job_cmd  in job_dict.items():
	
	print sleep_job_name," :  ",job_cmd + '\n'

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
	each_round.append(str(each_cmd[88:]))
	avg_each_round.append(sleep_job_name)
	for x in range(0, int(NO_OF_RUNS)):
		start_time = datetime.datetime.now()
		start = time.time()
		print "Round : " + str(x) + " for hadoop command : " + str(each_cmd[88:]) + " started at : " + str(start_time)
		os.system(each_cmd)
		end = time.time() - start
		end_time = datetime.datetime.now()
		print "Round : " + str(x) + " for hadoop command : " + str(each_cmd[88:]) + " ended at " + str(end_time)
		print("Total Time Taken for Round" + str(x) + " for hadoop command -> " + str(each_cmd[88:]) + " : " + str(end) + " seconds.") 	
		each_round.append(end)
	
	all_rounds.add_row(each_round)
	if int(NO_OF_RUNS) > 1:	
		avg_time_each_job = sum(each_round[2:])/(len(each_round)-2)
		avg_each_round.append(avg_time_each_job)
		graph_job_dict[sleep_job_name] = avg_time_each_job
	else:
		avg_each_round.append(each_round[1])
		graph_job_dict[sleep_job_name] = (each_round[1])
	
	avg_rounds.add_row(avg_each_round)


print("\n\n\n Summary of all rounds in seconds...\n")
try:
	print all_rounds	
except :
	print "Sorry cannot print summary of all rounds ... it appears that some values were missing"

print("\n Printing average of each run : Ignore first run's result and taking average of the rest \n")
try:
        print avg_rounds        
except :
        print "Sorry cannot print average of all rounds ...it appears that some values were missing" 

#Creating graph
for sleep_job_name, avg_time in graph_job_dict.items():

	try:
		file_name = (os.getcwd() + "/graph_plot/" + sleep_job_name)
		with open(file_name + ".plot", 'w+') as file:
			file.write("YVALUE=" + str(avg_time))		

	except IOError as err:
		print("Error occured while opening plot file" + str(err))
	




