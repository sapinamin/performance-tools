# coding: utf-8
#/usr/bin/python

import sys
import os
import re
import subprocess
import datetime
import time
import subprocess
import prettytable

if len(sys.argv) < 3:
	print("You have to enter No of Runs and Name of Database seperated by space ...Exiting System")
	sys.exit(0)
else:
	NO_OF_RUNS = sys.argv[1]
	NAME_OF_DATABASE = sys.argv[2]

print("\n\n")


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

print("\n")

#Get path of current working directory. If below parameters is uncommented, it will take current working directory of jenkins i.e./var/lib/jenkins/workspace/Hive-Stinger. For running a stand-alone python script, it is fine.
#current_path = os.getcwd()
current_path = "/grid/0/Stinger_tests"
#The query are located under test_queries directory
queries_path = os.path.join(current_path + "/test_queries")
print "Queries should be located under " + queries_path

init_sql_path = os.path.join(queries_path + "/init.sql")
print "init.sql query should be located as " + init_sql_path

#Declaration of dictionaries to save values of all rounds and minimum (fastest) amongst them 
final_values = {}
final_min = {}

print("\n")

#A sample for running a shell script in middle
#subprocess.call(['/grid/0/Stinger_tests/demo.sh'], shell=True)

#Getting list of all the query names under test_queries stored in variable named files 
files = os.listdir(queries_path)

#check is init.sql and other queries are located under /test_queries folder
if "init.sql" in files and len(files) > 1:
	for query_items in files:
		print "found " + query_items + " in " + queries_path
else:
	print "please make sure you have init.sql and other queries under " + queries_path + " ... Exiting System"
	sys.exit(0)


#Let's take of printing the header of table first
no_of_headers = int(NO_OF_RUNS)

#First value to be printed in header is 'Query'
header = ['Query']
header_fail = ['Query','FAIL MESSAGES']

#Rest of them are round numbers based on NO_OF_RUNS you entered ... eg R_1, R_2 etc.. the reason for it from 1 to NO_OF_RUNS+1 is because range starts with 0.
for i in range(1,no_of_headers + 1):
	header.append("R_"+str(i))

all_rounds = prettytable.PrettyTable(header)
all_rounds.align["Query"] = "l"
all_rounds.padding_width = 1

#get object of prettytable for fail messages
fail_round_table = prettytable.PrettyTable(header_fail)
fail_round_table.align["Query"] = "l"
fail_round_table.align_width = 1


#Here files is a list that contains name of query
for query in files:
	if query !='init.sql':		
		print("\n Execution begins for " + query)
		query_path = os.path.join(queries_path + "/" + query)
		#print query_path
		
		#creating a list and adding name of query as first element in each list to print it in prettytable
		each_round=[]
		each_round.append(query)

		#list to track fail messages
		fail_bool = True
		fail_each_round=[]
		fail_each_round.append(query)

		#x will keep tack of each round
		for x in range(0, int(NO_OF_RUNS)):

			#noting the start time with date and time info for each round
			start_time = datetime.datetime.now()
			print "\nRound : " + str(x) + " for query : " + query + " started at " + str(start_time)
		
			#noting the start timing to calculate the time difference between each round.
			start = time.time()

			#Below cmd runs you actual hive queries. If it does not work, uncomment the print(cmd) below and see the command generated.
			#Try printed command on your terminal screen. If it does not work than  it's problem with sourcing your env variables or paths are wrong.
			cmd = ("hive -i " + init_sql_path + " -f " + query_path + " -d DB=" + NAME_OF_DATABASE)
			
			#Print your entire cmd here that your shell is going to taking
			#print(cmd)

			#execting "cmd" in the terminal shell. stdout has all of its output
			#stdin=subprocess.PIPE is included so we have same shell terminal executing all hive queries.
			p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			
			#checking if each line in stdout has word Fetched in it because that line has final timings of hive query exection.
			for line in p.stdout.readlines():
				print(line.rstrip("\n"))
				if "Fetched:" in line:
    					print(line.rstrip("\n"))
					
					#If "Fetched" is present in output of hive logs, extract time from it.
					time_found = re.search('Time taken: (.+?) seconds, Fetched:',line)		
					
					#If time if found, add it to the each_round list
					if time_found:
						each_round.append(time_found.group(1))
				elif "FAILED:" in line:
					print(line.rstrip("\n"))
					each_round.append(10000)
					if fail_bool == True:
						fail_each_round.append(line)
						fail_bool = False
				
					
						
			#Checking if the "cmd" i.e. our hive query has finished succesfully. 			
			retval = p.wait()
			if retval ==0:
				print("Successfully finished executing the query")
			else:
				print(query + " did not completed succesfully ... but not exiting system as we have to execute other queries")
			
			
			#Below is a sample command to run something directly in python ... i.e. os.system(cmd) ... it will print everything in hive logs on screen
			#os.system("hive -i" + init_sql_path + " -f " + query_path + " -d DB=" + NAME_OF_DATABASE)
			
			#noting the end timing to calculate time difference between each round
			end = time.time() - start 
			end_time = datetime.datetime.now()
			print "Round : " + str(x) + " for query : " + query + " ended at " + str(end_time)
			
			#We are storing name of query and time taken in dictonary with key value pair. If we decide to use this, comment "each_round.append(query)"			   #around line 75
			final_values[query]=[each_round]
			#print final_values
			#print 'Total time taken which includes starting hive and including lib and jar : ' , (end_time - start_time)
			print "Total Time taken which includes starting hive and including lib and jars for " + query + " in Round " , x , " = " , ("%.2f"% (end)) , " seconds."
		
		#Adding the time taken by each round to all_rounds to print in prettytable
		try:
			all_rounds.add_row(each_round)
		except :
		#This is temp patch, if the script does not find Fetched or FAILED in hive logs than just append 10000. This not may always be true as in case of query34
			for x in range(0, int(NO_OF_RUNS)):
				each_round.append(10000)
			all_rounds.add_row(each_round)

		final_min[query]=[min(each_round)]

		#Adding fail message to prettytable object
		#fail_round_table.add_row(fail_each_round)

print("\n\n\n Summary of all rounds in seconds...\n")
try:
	print all_rounds
except :
	print "Sorry cannot print summary of all rounds ... appears that some values were missing"

#print("\n\n Summary of all ERROR MESSAGES...\n")
#print(fail_round_table)

#print values for all round stored in dictonary. If you intent to use it, comment line around 98 with each_round.append(query) and print all_rounds in above line
#print "Tests Results for " + NO_OF_RUNS + " Rounds "
#for k,v in final_values.iteritems():	
#	print "query : " , k + " : " , v

#Create plot files for jenkins
def create_plot_file(query_name, avg_time):

	try:		
		file_name = (os.getcwd() + "/graph_plot/" + query_name)[:-4]
		with open(file_name + ".plot" , 'w+') as file:
			if avg_time == '10000':
				file.write("YVALUE=" + '0')
			else:
				file.write("YVALUE=" + avg_time)
	except IOError as err:
		print("Error occured while opening plot file" + str(err))

#The fastest time is stored in dictonary as it may be latter used for graph plotting.
width=80
print("\n\nFastest Execution Analysis\n")
for k,v in final_min.iteritems():
	if str(v).strip('[]').replace("'","") == '10000':
		print str(k),' N/A'.rjust(width - len(k),'.')
	else:
		print str(k),str(v).strip('[]').replace("'","").rjust(width - len(k),'.') + " seconds."

        create_plot_file(str(k),str(v).strip('[]').replace("'",""))

#hwx-mrx-group@hortonworks.com - email to be sent



