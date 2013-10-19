# coding: utf-8
#/usr/bin/python

import sys
import os
import re
import shlex


class Hadoop1_Attempts_Report:

    import prettytable
    

    _master_header = ["JOBID", "JOBNAME"]
    _job_header_read = ["FINISHED_MAPS","FINISHED_REDUCES","FAILED_MAPS","FAILED_REDUCES","JOB_STATUS"]
    _task_attempt_header_read = ["TASKID","TASK_ATTEMPT_ID","TASK_STATUS","ERROR"]
    
    _job_header = _master_header + _job_header_read
    _job_table = prettytable.PrettyTable(_job_header)
    _job_table.padding_width = 1

    _attempts_header = _master_header + _task_attempt_header_read
    _attempts_table = prettytable.PrettyTable(_attempts_header)
    _attempts_table.padding_width = 1


   
    def job_print(cls, file_line):
    
        job_readings = []
        job_parent = []
        job_table_row = []

        for each_item in cls._job_header:
            job_table_row = job_table_row + ["N/A"]

        for line in file_line:
            if 'JOBID' in line:
                if 'JOBNAME' in line or 'FINISHED_MAPS' in line:
                    job_parent = shlex.split(line)
                    #print(job_parent)
                    for job_list in job_parent:
                        if '=' in job_list: 
                    
                            job_list = job_list.replace('"', '').strip()
                            (job_lb, job_rb) = job_list.split('=',1)
                            if job_lb in cls._job_header:

                                job_header_index = cls._job_header.index(job_lb)
                                job_table_row[job_header_index] = job_rb                            
                                                    
        return job_table_row  


    def task_attempt_print(cls, file_line, attempts_table, job_table_row):

        task_attempt_table_row = []
        task_attempt_parent = []

        for each_item in cls._task_attempt_header_read:
            task_attempt_table_row = task_attempt_table_row + ["N/A"]

        task_attempt_table_row.insert(0,job_table_row[0])
        task_attempt_table_row.insert(1,job_table_row[1])

        for line in file_line:
            if 'TASK_ATTEMPT_ID' in line and 'TASK_STATUS' in line:
                if 'TASK_STATUS="SUCCESS"' not in line:

                    line = line.replace('"', '').strip()
                    task_attempt_parent = line.split(" ")
                    for attempt_list in task_attempt_parent:
                        if "=" in attempt_list:
                            (attempt_lb, attempt_rb) = attempt_list.split("=")
                            #print(attempt_lb, " = " , attempt_rb)

                            if attempt_lb in cls._attempts_header:
                                task_attempt_header_read_index = cls._attempts_header.index(attempt_lb)
                                task_attempt_table_row[task_attempt_header_read_index] = attempt_rb
                    
                    cls._attempts_table.add_row(task_attempt_table_row)     


    def get_parse_history_job_logs(cls, history_file_location):

        job_table_blank_row = []
    
        try:
            with open(history_file_location, 'r') as history_file:
                file_line = history_file.readlines()

                job_table_row = cls.job_print(file_line)
                #job_table_row = [1,2,2,1,1,1,1]
                cls._job_table.add_row(job_table_row)

                cls.task_attempt_print(file_line, cls._attempts_table, job_table_row)          

        except IOError as err:
            print('File error' + str(err))

    def print_attempts_status(cls):

        print("\n JOB STATISTICS \n")
        print(cls._job_table)
        print("\n TASK ATTEMPT STATISTICS \n ")
        print(cls._attempts_table)

    def passHistorylogs_Path_Type(cls, history_logs_path, history_logs_type):
      
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
            print (e)


Hadoop1 = Hadoop1_Attempts_Report()
if len(sys.argv) < 2:
  print("Please enter the path where Hadoop1 history logs are located ...Exiting System")
  sys.exit(0)
else:
  hadoop1_history_logs_path = sys.argv[1]
  Hadoop1.passHistorylogs_Path_Type(hadoop1_history_logs_path,"default_*") 
  
#Hadoop1.passHistorylogs_Path_Type("/Users/samin/Performance/final_microsoft/tera_benchmark2/job_logs/history/version-1/ip-10-137-29-7.ec2.internal_1377032332828_/2013/08/21/000000/","default_*") 

