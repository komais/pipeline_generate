#! /usr/bin/env python3
'''
说明：
	任务状态有八种：
	* waiting : 已生成文件，但没有投递
	* qsub：已投递，尚未运行
	* running： 运行中
	* hold： hold住了
	* done： 在之前就跑完了，这次不用跑
	* finish： 这次跑完了
	* break： 断掉了
	* fail： 怎么投都错了，所以放弃了
更新说明：
2015年12月22日：
1. 使用subprocess中的wait来阻塞进程，防止qstat无法响应的状态，并使用poll检查返回值
2. 修复了failed状态又重新被修改成break的状态。
3. 增加了状态返回值，如果全部运行完，则为exit(0),否则为exit(1)
'''
import argparse
import sys
import os
import re
import time
import glob
import logging
import subprocess
import random
bindir = os.path.abspath(os.path.dirname(__file__))

__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')
def mylogger(script):
	logger = logging.getLogger('mylogger')
	logger.setLevel(logging.DEBUG)

	fh = logging.FileHandler('{0}.log'.format(script))
	fh.setLevel(logging.DEBUG)

	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	return logger

def popen(cmd):
	child = subprocess.Popen(cmd , shell=True , stdout = subprocess.PIPE)
	child.wait()
	if child.poll() == 0 :
		return [i.decode() for i in child.stdout]
	else:
		print('{0} is error\n'.format(cmd))
		debug_log.info('{0} is error\n'.format(cmd))
		if cmd.find('du') > -1 :
			return ['0']
		else:
			sys.exit(1)

class Job():
	def __init__(self , script, key , name ,max_cycle ):
		self.script = script
		self.key = key
		self.name = name 
		self.status = 'waiting'
		self.cputime_last = 0 
		self.max_cycle = max_cycle
		self.qsub_time = 0
		self.qhold_time = 0 
		self.qrls_time = 0
		self.io = []
		self.vmem = []
		self.maxvmem = 0 
		self.node = 'unknown'
	def add_atribute(self, queue, max_cycle, resource):
		self.queue = queue
		self.max_cycle = max_cycle
		self.resource = resource
	def qsub(self):
		cmd = 'cd {1} && qsub -cwd -S /bin/sh -q {0.queue} -l {0.resource} {0.script}'.format(self , os.path.abspath(os.path.dirname(self.script)))
		#print(cmd)
		qsub = "".join(popen(cmd))
		if qsub.startswith('Your job'):
			self.jobid = qsub.split()[2]
			self.status = 'qsub'
			self.time_qsub = time.time()
			self.qsub_time += 1 
		else:
			pass
	def qsub_to_run(self):
		self.time_last = time.time()
		self.status = 'running'

	def qhold(self):
		cmd = 'qhold {0.jobid}'.format(self)
		if os.system(cmd):
			pass
		else:
			self.status = 'hold'
			self.qhold_time = time.time()
	def qrlease(self):
		cmd = 'qrls {0.jobid}'.format(self)
		if os.system(cmd):
			pass
		else:
			self.status = 'running'
			self.qrls_time = time.time()
	def qdel(self):
		cmd = 'qdel {0.jobid}'.format(self)
		if os.system(cmd):
			pass
		else:
			self.status = 'break'
	def qstat(self):
		cmd = 'qstat -j {0.jobid}'.format(self)
		for i in popen(cmd):
			if i.startswith('usage'):
				tmp = i.rstrip().split(':' , 1 )[1]
				tmp = tmp.split(',')
				self.cputime_current = self.to_second(tmp[0].split('=')[1])
				self.io.append(tmp[2].split('=')[1])
				self.vmem.append( self.transfer(tmp[3].split('=')[1]) )
				self.maxmem = self.get_maxmem( self.transfer( tmp[4].split('=')[1] ))
	
	def to_second(self, cputime):
		tmp = [ int(i) for i in cputime.split(':') ] 
		if len(tmp) == 3 :
			return tmp[0]*3600 + tmp[1]*60 + tmp[2]
		elif len(tmp) == 4 :
			return tmp[0]*3600*24 + tmp[1]*3600 + tmp[2]*60 + tmp[3]

	def transfer(self , count):
		c_maxvmem = 0
		if count.endswith('G'):
			c_maxvmem = float(count.replace('G', '')) * 1e9
		elif count.endswith('M'):
			c_maxvmem = float(count.replace('M', '')) * 1e6
		elif count.endswith('K'):
			c_maxvmem = float(count.replace('K', '')) * 1e3
		elif count == 'N/A':
			c_maxvmem = 0 
		else:
			c_maxvmem = float(count)
		return c_maxvmem

	def get_maxmem(self , current_maxvmem):
		if self.maxvmem < current_maxvmem:
			self.maxvmem = current_maxvmem

	def slow_node(self):
		self.time_current = time.time()
		self.qstat()
		hold_time = int(self.qrls_time - self.qhold_time)
		if self.status == 'running' :
			if self.time_last == '':
				self.time_last = self.time_current
				self.cputime_current = self.cputime_last 
			if int(self.time_current - self.time_last) - hold_time  > 3600 : 
				if self.cputime_current - self.cputime_last - hold_time  < 10 :
					return True
				else : 
					self.cputime_last = self.cputime_current 
					self.time_last = self.time_current
		else:
			return False
	def __str__(self):
		debug_log.info('{0.name}\t{1}'.format(self , "|".join(self.io)))
		#print(self.vmem)
		try:
			v_io = sum([float(i) for i in self.io]) / len(self.io)
			print(self.io , io)
		except :
			v_io = '--'
		try:
			m_io = max([float(i) for i in self.io])
		except:
			m_io = '--'
		try:
			vmem = sum([i for i in self.vmem]) / len(self.vmem)/1e9
		except :
			vmem = '--' 
		self.maxvmem /= 1e9 
		return "{0.name}\t{0.jobid}\t{1}G\t{0.maxvmem}G\t{2}\t{3}\t{0.node}\n".format(self , str(vmem) , str(v_io) ,str(m_io))

def makedir(dir):
	if os.system('mkdir -p {0}'.format(dir)):
		sys.exit("cannot mkdir -p {0}".format(dir))

def generate_split_shell(shell , line_interval , job_prefix, max_cycle):
	pat = re.compile(';\s*;')
	qsub_dir = '{0}.{1}.qsub'.format(os.path.abspath(shell) , os.getpid())
	makedir(qsub_dir)
	job_count = 0 
	shell_out = '' 
	job_dict = {}
	with open(shell) as f_in:
		for line_count , content in enumerate(f_in):
			content = content.lstrip().rstrip().replace('&' , ';')
			if line_count % line_interval == 0 :
				if not shell_out == '' :
					shell_out.write('echo This-Work-is-Completed!\n')
					shell_out.close()
					debug_log.info('{0} close'.format(script_file))
				job_count += 1
				job_count_str = '{0:0>4}'.format(job_count)
				script_file = '{0}/{1}_{2}.sh'.format(qsub_dir , job_prefix, job_count_str) 
				shell_out = open(script_file , 'w')
				debug_log.info('{0} open'.format(script_file))
				job_dict[job_count_str] = Job(script_file , job_count_str , '{0}_{1}'.format(job_prefix, job_count_str) ,  max_cycle)
			if not bool(content) : continue
			if content.startswith('#'):continue
			content = content.rstrip(';')
			while(pat.search(content)):
				content = pat.sub(';',content)
			shell_out.write('{0} && '.format(content))
		else:
			shell_out.write('echo This-Work-is-Completed!\n')
			shell_out.close()
	return qsub_dir, job_dict

def modify_job_object( obj_dict , args):
	for i in obj_dict:
		a_job = obj_dict[i]
		#a_job.memeroy = args.memeory
		#a_job.p =args.thread
		a_job.add_atribute(args.queue , args.max_cycle, args.resource)

def parse_log_file(infile):
	finish_list = []
	with open(infile) as f_in:
		for i in f_in:
			tmp=i.rstrip().split('\t')
			if tmp[0].startswith(r'[Finished]:'):
				finish_list  += tmp[1:]
		return set(finish_list)

def check_obj_status(obj_dict , script_file):
	for a_file in glob.glob('{0}.*.log'.format(script_file)):
		finish_job = parse_log_file(a_file)
		#print(finish_job)
		for i in finish_job:
			obj_dict[i].status = 'done'

def is_dir_full(quota , dir):
	size =  "".join(popen('du -s {0}'.format(dir)))
	size = int(size.split()[0])
	qq = 0
	if quota.endswith('G') : 
		qq = int(quota.replace('G','')) * 1e9
	elif quota.endswith('M') : 
		qq = int(quota.replace('M','')) * 1e6
	elif quota.endswith('K') : 
		qq = int(quota.replace('K','')) * 1e3
	else:
		qq = quota
	if size >0.95*qq:
		return True
	else:
		return False

def check_die_node():
	die_node = []
	qhost = popen('qhost')
	for i,j in enumerate(qhost):
		if i > 2 :
			tmp = j.split()
			node_name , memory_use = tmp[0] ,tmp[5]
			for cc in  tmp[3:]:
				if cc.find(r'-') > -1 :
					die_node.append(node_name)
	return list(set(die_node))

def check_o_file(a_job):
	if os.path.isfile('{0.script}.o{0.jobid}'.format(a_job)):
		with open('{0.script}.o{0.jobid}'.format(a_job)) as f_in:
			content = "".join(f_in.readlines()).rstrip()
			if content.endswith('This-Work-is-Completed!'):
				return True
			else:
				return False
	else:
		return False

def check_running_job(all_job_list , bool_dir_full ):
	die_node = check_die_node()
	qstat = popen('qstat')
	running_stat = {}
	node_dict = {}
	for i,j  in enumerate(qstat):
		if i > 1 :
			tmp = j.split()
			running_stat[tmp[0]] = tmp[4]
			if tmp[4] == 'r' or tmp[4] == 'hr':
				node = tmp[7].split(r'@')[1]
				node = node.split(r'.')[0]
				node_dict[tmp[0]] = node
	
	#finish_job_list  = []
	count_running_jobs = 0 
	for a_job in all_job_list:
		if a_job.status == 'waiting' : continue
		if a_job.jobid in running_stat:
			if a_job.jobid in node_dict:
				a_job.node = node_dict[a_job.jobid]
				if a_job.node in die_node or a_job.slow_node(): 
					a_job.qdel()
					continue
			if running_stat[a_job.jobid] == 'hqw' or running_stat[a_job.jobid] == 'hr' or running_stat[a_job.jobid] == 'ht':
				count_running_jobs += 1 
			elif running_stat[a_job.jobid] == 'qw' or running_stat[a_job.jobid] == 't':
				count_running_jobs += 1 
			elif  running_stat[a_job.jobid] == 'r' :
				if a_job.status == 'qsub':
					a_job.qsub_to_run()
				count_running_jobs += 1 
			else :
				a_job.qdel()
			if bool_dir_full :
				if not a_job.status == 'hold' :
					a_job.hold()
				else:
					pass
			else:
				if a_job.status == 'hold' :
					a_job.release()
				else:
					pass
		else:
			if check_o_file(a_job):
				a_job.status = 'finish'
			else:
				if a_job.status == 'failed':
					pass
				else:
					a_job.status = 'break'
	return count_running_jobs 

def update_job_list(all ):
	unfinish = []
	for i in all:
		debug_log.info(i.status)
		if i.status in [ 'done' , 'finish' , 'failed']:
			pass
		else:
			unfinish.append(i)
	return unfinish

def output_log(logfile , done_job_list , obj_dict ):
	#print("done in" , [i.key for i in done_job_list])
	finish_job_list = []
	with open(logfile , 'a') as f_out:
		total , finish = 0 ,0 
		for i in obj_dict:
			total += 1 
			if obj_dict[i].status == 'done' or obj_dict[i].status == 'finish':
				finish += 1
				finish_job_list.append(obj_dict[i])
		
		if finish > len(done_job_list):
			f_out.write('[Process]:\t{0}/{1} finished\n'.format(finish , total))
			f_out.write('[Finished]:\t{0}\n'.format("\t".join([i.key for i in finish_job_list])))
			done_job_list = finish_job_list
	#print("done out " , [i.key for i in done_job_list])
	return done_job_list

def update_parameters(logfile):
	quota , maxjob = '',''
	with open(logfile) as f_in:
		for i in f_in:
			tmp = i.split()
			if i.startswith('DISK_QUOTA'):
				quota = tmp[1]
			elif i.startswith('Max_Jobs'):
				maxjob = int(tmp[1])
	return quota , maxjob

def guard_objs(obj_dict , args ,logfile):
	bool_dir_full = False 
	all_job_list = []
	if args.nodu:
		bool_dir_full = is_dir_full(args.quota , args.analysis_dir )
	for i in sorted(obj_dict):
		if not obj_dict[i].status == 'done':
			all_job_list.append(obj_dict[i])
	done_job_list = []
	while(all_job_list):
		for a_job in all_job_list:
			quota , maxjob = update_parameters(logfile)
			#print(quota , maxjob)
			if args.nodu:
				bool_dir_full = is_dir_full(quota , args.analysis_dir )
			while  bool_dir_full:
				count_running_job , finish_job_list  = check_running_job(all_job_list , bool_dir_full )
				done_job_list = output_log(logfile , done_job_list , obj_dict)
				time.sleep(args.interval)
				quota , maxjob = update_parameters(logfile)
				if args.nodu:
					bool_dir_full = is_dir_full(quota , args.analysis_dir )
			else:
				debug_log.info('{0.name} {0.status}'.format(a_job))
				count_running_job  = check_running_job(all_job_list , bool_dir_full )
				done_job_list = output_log(logfile , done_job_list , obj_dict)
				while count_running_job >= maxjob :
					time.sleep(args.interval)
					print('job number reach max , waiting for finish job : {0.name}'.format(a_job))
					quota , maxjob = update_parameters(logfile)
					if args.nodu:
						bool_dir_full = is_dir_full(quota , args.analysis_dir )
					debug_log.info('{0.name} {0.status}'.format(a_job))
					count_running_job  = check_running_job(all_job_list , bool_dir_full )
					done_job_list = output_log(logfile , done_job_list , obj_dict)
				else:
					if a_job.status == 'waiting':
						debug_log.info('{0.name} {0.status}'.format(a_job))
						a_job.qsub()
						debug_log.info('{0.name} {0.status}'.format(a_job))
					if a_job.status == 'break':
						with open(logfile, 'a') as f_out:
							f_out.write('{0.name} is not finish , it is break down , '.format(a_job))
							if not args.noreqsub:
								if a_job.qsub_time < a_job.max_cycle:
									a_job.qsub()
									f_out.write('reqsub it at the {0.qsub_time} time\n'.format(a_job))
								else:
									a_job.status = 'failed'
									f_out.write('reqsub {0.qsub_time} time exceed max cycle, drop it\n'.format(a_job))
								debug_log.info('{0.name} {0.status}'.format(a_job))
							else:
								f_out.write('pls reqsub it by yourself\n')
					else:
						print('{0.name} job is running , waiting....'.format(a_job))
		time.sleep(args.interval)
		debug_log.info('##in {0.name} {0.status}'.format(a_job))
		count_running_job   = check_running_job(all_job_list , bool_dir_full)
		debug_log.info('##in2 {0.name} {0.status}'.format(a_job))
		done_job_list = output_log(logfile , done_job_list , obj_dict)
		debug_log.info('##out {0.name} {0.status}'.format(a_job))
		if not args.noreqsub: 
			all_job_list = update_job_list(all_job_list )
			debug_log.info( "job list : " + '\t'.join( [ i.name for i in all_job_list]))
		else:
			while( not check_all_finish(all_job_list)):
				time.sleep(args.interval)
				count_running_job   = check_running_job(all_job_list , bool_dir_full)
				done_job_list = output_log(logfile , done_job_list , obj_dict)
			all_job_list = []

def check_all_finish(all):
	for i in all:
		#print(i.status)
		if i.status in ['waiting' , 'qsub', 'running', 'hold']:
			return False
	else:
		return True

def output_finish_log(obj_dict , logfile):
	with open(logfile ,'a') as f_out:
		failed_list = []
		for i in obj_dict:
			a_job = obj_dict[i]
			if a_job.status == 'finish':
				f_out.write(a_job.__str__())
			elif a_job.status == 'failed':
				failed_list.append(a_job.name)
			else:
				debug_log.info("finish {0.name} {0.status}".format(a_job))
		if len(failed_list) > 0:
			f_out.write('{0} is not finish\n'.format("\t".join(failed_list)))
			sys.exit(1)
		else:
			f_out.write('All jobs finished!')
			sys.exit(0)

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument(help='input file',dest='input')
	#parser.add_argument('-o','--output',help='output log  file',dest='output',type=argparse.FileType('w'),required=True)
	parser.add_argument('-l','--lines',  help='line number , default is [1] ',dest='line', type=int , default=1)
	parser.add_argument('-m','--maxjob',help='max job number , default is [4]', dest='maxjob', type=int ,default=4)
	parser.add_argument('-i','--interval',help='interval check time , default is [300]',dest='interval', type=int , default=300)
	parser.add_argument('-q','--queue',help='job queue , default is [sci.q]' , dest='queue', default='sci.q')
	parser.add_argument('-nr','--noreqsub',help='do not reqsub failed job ,default is reqsub', dest='noreqsub', action='store_true')
	parser.add_argument('-nc','--nocontinue',help='do not continue with unfinish log , default is continue',dest='nocontinue',action='store_false')
	parser.add_argument('-re','--resource',help='resouce list ,default is [ "vf=1G -l p=1" ] ', dest = 'resource',default=' vf=1G -l p=1 ')
	parser.add_argument('-prefix','--jobprefix', help='job prefix ,default is [work]',dest='prefix' ,default='work')
	parser.add_argument('-maxcycle','--maxcycle',help='max cycle , ,default is [5]', dest='max_cycle',default=5,type=int)
	parser.add_argument('-quota', '--quota', help='quota,default is [100000000000000000G]', dest='quota' , default='100000000000000000G')
	parser.add_argument('-analysis_dir', '--analysis_dir', help='analysis dir,default is [shell/../..]',dest='analysis_dir' )
	parser.add_argument('-nodu','--nodu',help='do not check disk,default is [du]',dest='nodu',action='store_false')
	args=parser.parse_args()
	
	#print(args.noreqsub)
	#sys.exit()
	work_dir = os.path.abspath(os.path.dirname(args.input))
	if args.analysis_dir == None:
		args.analysis_dir = '{0}/../../'.format(os.path.abspath(work_dir))
	global debug_log
	debug_log = mylogger(args.input)
	logfile = '{0}.{1}.log'.format(args.input , os.getpid())
	with open(logfile,'w') as f_out:
		f_out.write('DISK_QUOTA\t{0}\nMax_Jobs\t{1}\n'.format(args.quota, args.maxjob))
	shell_dir , obj_dict  = generate_split_shell( args.input , args.line , args.prefix , args.max_cycle)
	debug_log.info('generate shell done')
	modify_job_object( obj_dict , args )
	#print( obj_dict )
	if args.nocontinue:
		check_obj_status(obj_dict ,  args.input)
	debug_log.info('modify object status done')

	guard_objs(obj_dict , args , logfile)
	output_finish_log(obj_dict , logfile)


if __name__ == '__main__':
	main()
