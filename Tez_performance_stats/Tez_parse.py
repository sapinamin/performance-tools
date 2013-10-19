# coding: utf-8
#/usr/bin/python

import sys
import os
import re
import csv
import glob
from collections import defaultdict
import shlex
import prettytable
import subprocess

master_header = ["dagId","vertexName","vertexId","taskId","taskAttemptId","startTime","finishTime","timeTaken", "status", "RACK_LOCAL_TASKS"]

tez_basic_table = prettytable.PrettyTable(master_header)
tez_basic_table.padding_width = 1

def tree(): 
  return defaultdict(tree)

#def dicts(t): 
#  return {k: dicts(t[k]) for k in t}

def add(t, keys):
  for key in keys:
    t = t[key] 

def add_vertex(t, keys):
  for key in keys:
    t = t[keys]  

def add_single_item(t, keys):
  t = t[keys]


dag_id_list = []
tez_dict = tree()

def attempt_print(file_line):
  
  for line in file_line:    
    #if '[HISTORY]' in line:      
      final_line = outer_bracket_reg_expression(line)  
      #print(final_line)      
      
      if '[Event:DAG_STARTED' in line:
        #final_line = outer_bracket_reg_expression(line)
        dag_id_list.append(get_dag_id(final_line)) 
        add_single_item(tez_dict, get_dag_id(final_line))
       
  #for line in file_line:                 
      '''
      if 'Event:VERTEX_STARTED' in line:  
        #final_line = outer_bracket_reg_expression(line)
        #print(final_line)
        dag_id, vertex_id = get_dag_vertex_id(final_line)  
        for k, v in tez_dict.iteritems():
          if str(k) == dag_id:
            vertex_string = (str(k) + ',' + vertex_id).split(",")      
            add(tez_dict, vertex_string)  
              
      if 'Event:TASK_STARTED' in line:
        #final_line = outer_bracket_reg_expression(line)
        dag_id, task_id = get_dag_task_id(final_line) 
        task_vertex_string = 'vertex'+ task_id[4:-7]                    
        task_string = (dag_id + ',' + task_vertex_string + ',' + task_id).split(",")
        add(tez_dict, task_string)
      '''  
      if 'Event:TASK_ATTEMPT_STARTED' in line:
        #final_line = outer_bracket_reg_expression(line) 
        dag_id, task_attempt_id = get_dag_task_id(final_line)
        task_vertex_string =  'vertex'+ task_attempt_id[7:-9] 
        task_id_string = 'task' + task_attempt_id[7:-2]
        task_attempt_string = (dag_id + ',' + task_vertex_string + ',' + task_id_string + ',' + task_attempt_id).split(",")
        add(tez_dict, task_attempt_string)
        
  #print(dicts(tez_dict))

def print_dict(dictionary, file_line, ident = '', braces=1):

  for key, value in dictionary.iteritems():
        if isinstance(value, dict):
            #print '%s%s%s%s' %(ident,braces*'[',key,braces*']') 
            
            if "dag" in key:
              #print '%s%s%s' %(ident,'->',key) 
              get_readings(key, file_line, 'Event:DAG_FINISHED')
            
             
            if "vertex" in key: 
              get_readings(key, file_line, 'Event:VERTEX_FINISHED' )         
              #print '%s%s%s' %(ident,'->',key) 

            if "task" in key:
              get_readings(key, file_line, 'Event:TASK_FINISHED')
              #print '%s%s%s' %(ident,'->',key)
              
            if "attempt" in key:
              get_readings(key, file_line, 'Event:TASK_ATTEMPT_FINISHED')  
              #print '%s%s%s' %(ident,'->',key)
                          
            print_dict(value, file_line, ident+'  ', braces+1)
        #else:
        #    print ident+'%s = %s' %(key, value)

def get_readings(each_id, file_line, line_identifier):
  
  if file_line:
    for line in file_line:
      if each_id in line and line_identifier in line:
        final_list = get_readings_list(each_id, file_line, line)
        #print(final_list)
        parse_final_list(final_list)

def get_readings_list(id, file_line, line):
    final_line = outer_bracket_reg_expression(line)
    parse_line = final_line.strip("[").replace("][", ',')
    parse_line2 = parse_line.replace("]: ", ", ")
    final_list = parse_line2.split(",")
    del final_list[0]
    del final_list[0]
    return final_list                 

def parse_final_list(final_list):

  table_add_row = []
  for each_header in master_header:
    table_add_row.append(' ')

  if final_list:
    for each_item in final_list:
      if "=" in each_item:
        item_lb, item_rb = each_item.split('=', 1)
        item_lb = item_lb.strip()
        if item_lb in master_header:
          header_item_index = master_header.index(item_lb)
          table_add_row[header_item_index] = item_rb

  add_to_prettytable(table_add_row)        

def add_to_prettytable(table_add_row):

  if table_add_row:
    try:
      tez_basic_table.add_row(table_add_row)
    except Exception,e:
      print("Sorry was not able to print basic Tez Statistics : " +  str(e))       

def outer_bracket_reg_expression(line):
  final_line = (line[line.rfind("[HISTORY]") + int(len("[HISTORY]")):])
  return final_line

def inner_split_timings(line):  
  parse_line = line.strip("[").replace("][", ',')
  parse_line2 = parse_line.replace("]: ", ", ")
  
def get_dag_id(line):
  parse_line = line.strip("[").replace("][", ',')
  parse_line2 = parse_line.replace("]: ", ", ")
  final_dag_list = parse_line2.split(",")
  dag_name, dag_id = final_dag_list[0].split(":")
  return dag_id  

def get_dag_vertex_id(line):
  parse_line1 = line.replace("]: ", ",")
  dag_event_info, vertex_info = parse_line1.split(',',1) 
  dag_id = parse_dag_id_line(dag_event_info)
  vertex_list = vertex_info.split(",")
  vertex_id_lb, vertex_id_rb = vertex_list[1].split('=')
  return dag_id, vertex_id_rb
  
def parse_dag_id_line(line):
  outer = re.compile("\[(.+)\]")  
  line_strip_outer_bracket = outer.search(line)
  dag_tag, dag_id = line_strip_outer_bracket.group(1).split(':')
  return dag_id

def get_dag_task_id(line):
  parse_line1 = line.replace("]: ", ",")
  dag_event_info, task_info = parse_line1.split(',',1) 
  dag_id = parse_dag_id_line(dag_event_info)
  task_list = task_info.split(",")
  task_id_lb, task_id_rb = task_list[1].split('=')
  return dag_id, task_id_rb

def create_tmp_file(file_line, tez_file_location, files):
  
  tmp_file_name = tez_file_location + ".tmp" 
  if os.path.exists(tmp_file_name):
    os.remove(tmp_file_name)

  try:
    with open(tmp_file_name, 'w+') as file_tmp:
      for line in file_line:
        if '[HISTORY]' in line:
          file_tmp.write(line)

  except IOError as err:
    print('File error in creating tmp file' + str(err))
  return tmp_file_name  

def print_table():
  print("\n BASIC TEZ STATISTICS \n")
  print(tez_basic_table)  

def get_parse_tez_job_logs(tez_file_location, files):

  job_table_blank_row = []
  
  try:
      with open(tez_file_location, 'r') as tez_log_file:

        file_line = tez_log_file.readlines() 
        tmp_file_name = create_tmp_file(file_line, tez_file_location, files)
        
        with open(tmp_file_name, 'r') as tmp_log_file:
          tmp_file_line = tmp_log_file.readlines()
          
          attempt_print(tmp_file_line)                  
          print_dict(tez_dict, tmp_file_line)  

          print_table()              

  except IOError as err:
      print('File error' + str(err))

def passTezlogs_Path_Type(Tez_logs_path):

  try:
      for r,d,f in os.walk(Tez_logs_path):
          if Tez_logs_path:
            for files in f:
              #if "484" in files and '.tmp' not in files:
              #if "failed" in files and '.tmp' not in files:
              #if "singleSuccessful" in files and '.tmp' not in files:
              if '.tmp' not in files:
                files_path = os.path.join(r,files)
                get_parse_tez_job_logs(files_path, files)
                print("parsing done for : " + files_path)

      #cls.print_attempts_status()                
                     
  except Exception,e:
      print (e)

if len(sys.argv) < 2:
  print("Please enter the path where tez logs are located ...Exiting System")
  sys.exit(0)
else:
  Tez_logs_path = sys.argv[1]
  passTezlogs_Path_Type(Tez_logs_path) 

#Tez_logs_path = "/Users/samin/history_logs/Tez/tezlogs/newlogs/"



