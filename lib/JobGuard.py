import threading
import time
import queue
import subprocess
import sys
import os
import re
import signal

def myTime():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


class MyThread(threading.Thread):
	def __init__(self ,  thread_name , logfile, cmd ,major ):
		threading.Thread.__init__(self)
		#self.lock = lock
		self.name = thread_name
		self.cmd = cmd
		self.logfile = logfile
		self.major = major
		self.status = True
	def run(self):
		#self.lock.acquire()
		self.logfile.write('[start]: {0} start at {1}\n'.format(self.name , myTime()))
		self.logfile.flush()
		print(self.cmd)
		if subprocess.call(self.cmd , shell=True) != 0 : 
			self.logfile.write('[break]: {0} break down at {1}\n'.format(self.name , myTime()))
			self.logfile.flush()
			self.status = False
			#sys.exit()
		#if self.status == True:
		else:
			self.logfile.write('[finish]: {0} finish at {1}\n'.format(self.name , myTime()))
			self.logfile.flush()
		#self.lock.release()

def run(jobs):
	for order in sorted(jobs):
		m = []
		for a_job in jobs[order]:
			if a_job == '' : continue
			a_job.start()  
			if a_job.major == True : m.append(a_job)
		for a_job in m:
			a_job.join()
		for a_job in jobs[order]:
			if a_job == '' : continue
			if a_job.status == False and a_job in m :
				sys.exit("Sorry,goodbye")

def RemoveFinish(jobs, finish):
	for order in jobs:
		for count, a_job in enumerate(jobs[order]):
			if a_job.name in finish:
				jobs[order][count]=''
	return jobs

def ReadLog(logfile):
	pat1 = re.compile('^\s+$')
	finish = []
	if not os.path.isfile(logfile):
		return finish
	else:
		with open(logfile,'r') as f_file:
			for line in f_file:
				if line.startswith('#') or re.search(pat1,line):continue
				tmp=line.rstrip().split()
				if line.startswith('[finish]:'):
					finish.append(tmp[1])
		return finish

def signal_term_handler(signal , frame , logfile ):
	logfile.write('[break]: system was break down at {0}\n'.format( myTime()))
	sys.exit(0)




