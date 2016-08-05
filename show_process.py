#! /usr/bin/env python3
import argparse
import sys
import os
import re
import glob
bindir = os.path.abspath(os.path.dirname(__file__))
home_dir =  os.environ['HOME']
__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s*$')

def searchDir(indir):
	all_shell = glob.glob('{0}/*.sh'.format(indir))
	job_tree = {} 
	for i in all_shell:
		bn = os.path.basename(i)
		if not bn[0].isnumeric() : continue
		tmp = bn.split('_' , maxsplit = 2 )
		#print(tmp)
		if not len(tmp) == 3 : continue
		level1 , level2 = int(tmp[0]) , int(tmp[1])
		if not level1 in job_tree:job_tree[level1] = {}
		if not level2 in job_tree[level1]:
			job_tree[level1][level2] = {}
		job_tree[level1][level2]['shell']  = i 
		job_tree[level1][level2]['name']  = bn.replace('.sh','') 
	r_list = []
	for i in sorted(job_tree):
		for j in sorted(job_tree[i]):
			r_list.append(job_tree[i][j]['shell'])
	return r_list, job_tree

def readTotalLog(logfile , job_tree):
	with open(logfile) as f_log:
		for line in f_log:
			if line.startswith('#') or re.search(pat1,line):continue
			job_name = line.split()[1]
			tmp = job_name.split('_')
			level1 , level2 = int(tmp[0]) , int(tmp[1])
			if  level1 in job_tree and level2 in job_tree[level1]:
				job_tree[level1][level2]['status'] = ''

				if line.startswith('[start]:'):
					job_tree[level1][level2]['status'] = 'start'
				elif line.startswith('[finish]:'):
					job_tree[level1][level2]['status'] = 'finish'
				elif line.startswith('[break]:'):
					job_tree[level1][level2]['status'] = 'break'
			else:
				print("Error: {0} is not exist in shell directory".format(job_name))
	return job_tree

def chooseNew(all_log):
	if len(all_log) == 1:
		return all_log[0]
	elif len(all_log) > 1 :
		a_log = all_log[0]
		for i in all_log[1:]:
			if os.path.getmtime(i) > os.path.getmtime(a_log):
				a_log = i
		return a_log
	else :
		return -1 


def readMissionLog(shell):
	all_log = glob.glob('{0}.*.log'.format(shell))
	is_hold = 0 
	running_log = chooseNew(all_log)
	if running_log == -1 :
		return '0' , 0
	else:
		schedual = '0' 
		with open(running_log) as f_log:
			for line in f_log:
				if line.startswith('[Process]'):
					schedual = line.split()[1]
				elif line.startswith('[Job Hold]'):
					is_hold = 1 
				elif line.startswith('[Job Release]'):
					is_hold = 0
		return (schedual , is_hold)

def showProcess(name , dir):
	logfile = '{0}/shell/log.txt'.format(dir)
	shell_dir = '{0}/shell'.format(dir)
	total_mission , job_tree = searchDir(shell_dir)

	mission_schedual = readTotalLog(logfile , job_tree)

	for l1 in job_tree:
		for l2 in job_tree[l1] :
			(job_tree[l1][l2]['schedule'] , job_tree[l1][l2]['hold']) = readMissionLog(job_tree[l1][l2]['shell'])
	
	#print(job_tree)
	running_job = ''
	unfinish_flag = 0 ## whether finish or not
	start_flag = 0 ## whether running or not  
	hold_flag = 0 
	status = 'running'
	for l1 in job_tree:
		for l2 in job_tree[l1]:
			a_job = job_tree[l1][l2]
			if 'status' in a_job:   ## log file is record 
				start_flag = 1 
				if a_job['hold'] == 1 : hold_flag = 1 
				if a_job['status'] == 'start':  ## start in logfile 
					unfinish_flag = 1
					running_job += '{0};{1} |'.format(a_job['name'], a_job['schedule'])
				elif a_job['status'] == 'break': ## break in logfile
					unfinish_flag = 1
					running_job += '{0};{1} |'.format(a_job['name'], a_job['schedule'])
					status = 'break at {0};{1}'.format(a_job['name'], a_job['schedule'])
				elif a_job['status'] == 'finish':
					pass
			else:
				running_stat = 1 


	if running_job == '' : running_job = '**'
	running_job = running_job.rstrip(' |')
	if start_flag == 0 : 
		status = 'Ready for Run'
		running_job = 'PLAN'
	if unfinish_flag == 0 and start_flag == 1: 
		status = 'finish'
		running_job = 'END'
	if hold_flag == 1 and (not status.find('break') > -1  ):
		status = 'Hold'
	output_list = [ name , str(len(job_tree.keys())), running_job , status , dir]
	if status.find('break') > -1 :
		print("\033[1;31;40m" + "\t".join(output_list) + '\033[0m')
	elif status.find('Hold') > -1 :
		print("\033[1;31;40m" + "\t".join(output_list) + '\033[0m')
	elif status.find('running') > -1  :
		print("\033[1;32;40m" + "\t".join(output_list) + '\033[0m')
	else  :
		print("\033[1;37;40m" + "\t".join(output_list) + '\033[0m')
	#print("033[0m")
	
def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input file',dest='input',default='{0}/.mission/.pipeline.log'.format(home_dir))
	#parser.add_argument('-o','--output',help='output file',dest='output',type=argparse.FileType('w'),required=True)
	args=parser.parse_args()

	print("Name\tTotal_target\tScheduel\tStatus\tWork_dir")
	print("-"*100 )
	if not os.path.isfile(args.input) :
		print( "\033[1;33;40m Please choose a log file ,{0} is not existed \033[0m\n ".format(args.input))
		sys.exit()
	with open(args.input) as f_input:
		for line in f_input:
			if line.startswith('#') or re.search(pat1,line):continue
			tmp=line.rstrip().split('\t')
			project_name , project_dir = tmp[0] , tmp[1]
			if not os.path.isdir(tmp[1]):
				print("\033[1;31;40m" + "{0}\t--\tError\tcannot found directory\t{1}".format(tmp[0],tmp[1]) + '\033[0m')
			else:
				showProcess(project_name , project_dir)
	print()

if __name__ == '__main__':
	main()
