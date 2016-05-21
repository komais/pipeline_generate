#! /usr/bin/env python3
'''
程序描述：
	在串流程的时候，其实是分为三步的，第一步实现每一个小脚本，第二步把相关的小脚本组合成一个模块，完成一个分析功能；第三步，把这些模块类似搭积木一样，进行组合，得到实现不同目标的流程。
	这个程序，就是在大家得到模块后，帮助排列组合的工具。
参数说明：
	-i ： 输入的配置文件，包含如何组合的规则
	-o ： 输出流程的目录
输入文件：搭积木的规则
	包含很多个不同的分析模块，每一模块的其实用[Job Start],终止用[Job End]。在每个模块内部，每一行包括标识符，然后tab分割，相应的值。
	例如：
	Name : 这个分析模块的名称
	Memory：该分析模块的功能，会按照这个设置进行申请内存。如果不能确定，可以先预设得大一些，测试一下流程后，去${OUTDIR}/shell/下去查看相应模块的运行log文件，文件包含该步在计算节点上的内存大小。默认为3G
	Queue : 使用哪个队列，例如组装可以使用super.q,其余的使用sci.q，默认是sci.q
	CPU :　对于这个模块的最大并行任务数，可以是一个整数，那么每次运行则以固定的个数进行投递；也可以是N，表明投递时，同时投递所有的样品；也可以是N/2,N/3,表示为样品数的1/2或者1/3进行投递任务,默认为3
	Major： 是否是主要进程.T - 主干任务， F - 支线任务。大家都玩过游戏，主干任务表示必须得完成这个任务，才能进行下一个任务；而支线任务则不需要。默认是Major
	Order: 第几级任务。
	Thread: 每个任务投递时的线程数，默认不加的时候是1，如果在进行blast或者比对的时候建议根据实际情况加入
	Qsub : 是否投递任务，True使用qsub投递，False直接后台运行，默认是True
	Node:  在False的情况下，投递到那个节点，默认是''
	Command： 命令，如果是多行命令，可以一直往下写，直到遇到[Job End]
	其他：
	OUTDIR：保留字，与最终生成程序的args.outdir对应
	BIN ： 保留字，与最终程序中args.bin对应
	LOGFILE： 保留字，与最终程序中的args.logfile对应
	$(sample) : 表示生成的流程会对args.input 该块的元素进行循环,如果该块有10行，那么会投递10个任务
	Para：生成的流程会对 args.input 中以Para_ 开头的元素支持内插
	DB: 生成的流程会对args.input 中以DB_ 开头的元素也支持内插
	例如：
[Job Start]
Name    MergeFq
Memory  1G
Queue   sci.q
CPU     N
Major   T
Order   1
Thread	5
Qsub	False
Node	c0008
Command make -f BIN/MergeFq/makefile indir=$(sample)[5]/$(sample)[1] outdir=OUTDIR sample_id=$(sample)[0] log_file=LOGFILE LinkFqPara_seq
[Job End]
	上面这个任务处于第一层，major是T，表明要运行第二层任务，必须需要这个任务完成。
输出文件：
	pipeline.py :会在outdir下生成一个pipeline.py , 可以用来运行程序。详细使用见pipeline.py -h
	lib,src : 配置目录，一言难尽
	config_example.txt ： pipeline.py的输入文件的模板。其中[]里面表示一个文本块的起始，直到遇到另一个文本块或者文件结束。其中分成两类：
	一类是[Para],[DB]，这一类是使用该块中变量对pipeline.py中的相应程序进行变量的定义
	另一类是其他名称，例如[sample],表示用该块里面的每一行的相应列进行循环生成脚本，投递任务。
更新说明：
	2015年10月8日
	* 添加了Thread参数，会在qsub-sge.pl中指定线程数，默认为1。 调用的是qsub里面的 -l p=1 。 p为线程数，而不是cpu数。 
	  需要注意的是，如果这个值设置的过大，比如10，会导致任务排队时间过长，一般建议在4以下。
	2015年10月14日
	* 删除了 -n 参数，将nohup或者qsub的设置移到config文件中，默认每个任务都是qsub上去，如果有需要设置成登陆节点运行，需要设置Qsub	False
	2016年1月29日
	* 将qsub-sge.pl 转成 qsub_sge.py
	2016年3月30日 v5.6
	* 修改了qsub_sge.py , 修复了du可能无响应的bug，降低了qhost和qstat的频率
	* 将args.bin的绝对路径作为一个值保存下来，存在log.txt中，以便后续查询。
	2016年4月10日 v5.7
	* 添加了Node参数，在非qsub的情况下，可以将任务投递到Node设定的节点后台运行。
To Be Done:
	* 没想到 
'''
# -*- coding: utf-8 -*-  
import argparse
import sys
import os
import re
bindir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('{0}/lib'.format(bindir))
import parseConfig

__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')
pat2=re.compile('\$\((\S+?)\)(\[\d+\])')
pat3 = re.compile('(Para_[A-Za-z0-9_-]+)\\\\?')
pat4 = re.compile('(DB_[A-Za-z0-9_-]+)\\\\?')
ReserveWord = ['OUTDIR','BIN','LOGFILE']
class Job:
	def __init__(self , name ) :#, memory , time, command, export, part):
		self.name = name
		self.Memory = '3G'
		self.Time = '10h'
		self.CPU = '3'
		self.Queue = 'sci.q'
		self.Export = ''
		self.Major = True
		self.Part = ''
		self.Thread = 1
		self.Qsub = True
		self.Node = ''
	def addAtribute(self , key, value):
		if key in ['Name','Memory','Time','CPU','Export','Command','Part','Order','Queue' , 'Thread' , 'Qsub' , 'Node' ]:
			self.__dict__[key] = value
		elif key == 'Major':
			if value == 'T' or value == 'True':
				self.__dict__[key] = 'True'
			elif value == 'F' or value == 'False':
				self.__dict__[key] = 'False'
			else:
				print('Major	{0} is error,set True'.format(value))
		else:
			print('{0} is useless'.format(key))
	def format_command(self):
		output = ''
		tt = [] 
		for i in self.Command:
			mm = [] 
			for j in i.split():
				if pat2.search(j):
					j = pat2.sub(r'{\1\2}',j)
				if pat3.search(j):
					j = pat3.sub(r"{para[\1]}",j)
				if pat4.search(j):
					j = pat4.sub(r"{db[\1]}",j)
				for i in ReserveWord:
					j = j.replace(i,'{{{0}}}'.format(i))
				mm.append(j)
			tt.append(" ".join(mm))
		output = " \\n ".join(tt) 
		#print(output)
		return [ len(tt),output]
	def output_info(self):
		part = ''
		para = [] 
		db = []
		for i in self.Command:
			if pat2.search(i):
				part = pat2.search(i).group(1)
			if pat3.search(i):
				para += pat3.findall(i)
			if pat4.search(i):
				db += pat4.findall(i)
#				para.append(pat3.search(i).group(1))
		return part,para,db

def ParseOneJob(content):
	'''command should be last attribute of a part
	'''
	cmd = []
	name , memory , time , export ,part = '', '','','' , ''
	for line in content:
		tmp = line.rstrip().split("\t")
		match =  pat2.search(line)
		if part == '' and match: part = match.group(1)
		if len(tmp) == 2 :
			key ,value = tmp[0], tmp[1]
			if key == 'Name':
				name = value
				a_job = Job(name)
			elif key == 'Command':
				#print(value)
				cmd.append(value)
			else:
				a_job.addAtribute(key, value)
		elif len(cmd) > 0 :
				cmd.append(line.rstrip())
	a_job.addAtribute('Command',cmd)
	if part != '':a_job.addAtribute('Part',part)
	#print(type(a_job),a_job.name)
	return a_job

def ReadJob(f_file):
	content = []
	job_list = {}
	for line in f_file:
		if line.startswith('#') or re.search(pat1,line):continue
		#tmp = line.rstrip().split("\t")
		if line.startswith(r'[Job Start]'):
			content = []
		elif line.startswith(r'[Job End]'):
			a_job = ParseOneJob(content)
			order = int(a_job.Order)
			if not order in job_list:
				job_list[order] = []
			job_list[order].append(a_job)
		else:
			content.append(line)
	return job_list 

def output(jobs , f_out):
	script = '''
\'''
Parameters:
	-i , --input : 输入的项目配置文件
	-b , --bin : 程序调用的相关程序路径，与流程配置文件中的${BIN}对应
	-t , --thread: 线程数，如果定义了这个，那么会覆盖流程配置文件中的CPU
	-q , --queue : 指定的队列，如果定义了这个，则会覆盖流程配置文件中的队列
	-o , --outdir: 输出目录，与流程配置文件中的$(OUTDIR)对应
	-name,--name : 任务名称
	-j,   --jobid: 任务前缀，默认为name
	-r,   --run  : 是否自动投递任务，默认为不投递任务，但会在${OUTDIR}/shell中生成脚本，可以进行检查
	-c,   --continue: 在qsub下有效（设置了-r，但不设置-n），如果某一步分析中没有完成全部任务，如果不指定则从头运行该步所有任务，指定则完成该步剩余未完成任务
	-n,   --nohup : 默认是qsub运行任务，设置则为nohup
	-a,   --add   : 是否只运行加测的样品，如果指定则只运行加测的样品。
	-quota, --quota : 分析目录的配额，是之前找文明申请的大小。默认是1000G，请根据实际情况进行调整。
说明：
	Q: 支线任务断了咋办？
	A: 支线任务断了，主线任务会继续运行，而整个流程不受影响。但是查看show_process的时候，可以看到break的状态。而break的等级是高于run和hold的，因此，需要自行判定程序是否完成。

	Q: 主线任务断了咋办？
	A: 主线断了，那么就断了，需要重新投递。
	   如果主线任务断了，而支线任务没有完成，那么主程序会等候支线任务完成才会实现最终的退出，所以会导致程序一直在运行的假象。这个时候有两个处理办法：
	   1. 把所有进程都杀掉，然后重新投递
	   2. 只杀掉主进程（pipeline.py)，并且在log.txt中人为添加支线任务的finish标识（防止再次运行pipeline.py时重新投递），之后重新运行该任务。
	Q:如何识别加测样品？
	A:当某一块出现两次或者多次，那么最后一次出现的内容作为加测项

	Q:如何监控项目运行状态？
	A:运行/annoroad/bioinfo/PMO/liutao/pipeline_generate/bin/v5/show_process.py会显示项目的状态。项目运行状态分为running, break, plan, end 四种。其中running表示正在运行，break表示中断，plan表示准备运行，end表示运行完成,hold表示磁盘不够，任务挂起。

	Q: 发现任务状态是break，该咋办？
	A: 当发现任务状态是break的时候，首先需要确定break掉的任务是否是主线任务。
	   如果是主线任务，如果主程序自然退出(在top或者ps的时候没有发现pipeline.py），则可以重新投递任务；
	                   如果没有自然退出（主程序pipeline.py还在运行），那么可能是有之前的支线任务未完成，可以参照Q2来进行操作；
	   如果是支线任务，如果查看log.txt发现主线任务全部完成或者正常运行，如果时间允许，可以等待所有任务完成后再投递任务；
	                                                                    如果加急，可以把该步对应的sh文件修改后，手动投递该任务；
	                   如果主线任务也断掉了，那么修改脚本后，重新投递所有任务。

	Q:监控项目的记录文件在哪？
	A:程序会在您的home目录下，生成一个记录文件，路径为~/.mission/.pipeline.log，记录了每个项目的分析目录。如果不想显示某个项目，可以对相应的行进行删除或者编辑。
	Q:如果程序断了，咋办？
	A:如果程序由于各种因素中断了，仔细检查脚本，如果脚本没错，确定只是中断，那么重新运行一次之前的脚本，默认会把断掉的模块全部重头运行；如果不想将该模块内已经完成的样品重新运行，可以加上-c参数，那么会只运行没有运行成功的样品。
	Q:如何发现配额不够？
	A:如果配额不够了，会把所有的任务挂起，使用show_process查看时，会发现Hold状态；或者使用qstat的时候，会发现hqw，hr，ht等，或者没有任务在运行（因为配额不够，程序会自动不投递任务）
	Q: 配额不够了，咋办？
	A: 第一，找文明修改配额 ； 第二，修改相应的sh.*.log文件，加入一行DISK_QUOTA\t**G ;之后，程序会自动的release。但需要注意的是，因为之前在程序里设置了较小的配额，所以之后每一步都会被hold。所以需要之后每个log文件都加上 DISK_QUOTA\t**G，来每次进行更改；或者杀掉重新来。 
	   或者删除文件来释放空间，这样的话，后面可以不用修改就可以运行。
	Q: 如何杀掉程序？
	A: 1. 杀掉所有的子进程 守护进程qsub_sge.pl，否则的杀掉的任务会重新投递；
	   2. 杀掉所有的任务 qdel掉
	Q: 如何精准的杀掉守护进程？
	A: 1. 在重新投递之前，使用ps -f -u name |cat 然后仔细的判别，获得进程ID
	   2. 查看shell后面的数字，在sh 和log直接的数字，是进程ID，使用kill -9可以杀掉
更新说明：
	2015-8-17
	1.之前一个target断掉后，需要对这个target里的所有任务重新投递；目前加上了-c 参数，可以选择跳过已运行完毕的任务
	2015-9-1
	1. 加入quota参数，如果目录配额不够，会自动挂起任务；
	2. 加入sh.*.log文件中的节点的记录
	3. 加入了maxcycle，可以在pipeline.py对maxcycle中进行修改
	4. 修改支线任务断掉，不影响主线任务整体运行。
	2015-10-8
	1. 添加了每个任务投递时的线程数，在最初的config文件中使用Thread控制，默认为1.
	2016-03-26
	1. 在shell下面的log文件中，添加了使用程序版本的记录，方便以后升级时，可以进行查找。
\'''
#! /usr/bin/env python3
# -*- coding: utf-8 -*-  
import argparse
import sys
import os
import re
bindir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('{0}/lib'.format(bindir))
import parseConfig
import JobGuard

__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\\t{0}\\nmail:\\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input file',dest='input',type=open,required=True)
	parser.add_argument('-b','--bin',help='bin dir',dest='bin',default=os.path.dirname(bindir))
	parser.add_argument('-t','--thread',help='thread number ',dest='thread',type=int)
	parser.add_argument('-q','--queue',help='computer queue name ',dest='queue')
	parser.add_argument('-o','--outdir',help='output file',dest='outdir',required=True)
	parser.add_argument('-name','--name',help='project name',dest='name',required=True)
	parser.add_argument('-j','--jobid',help='job id prefix',dest='jobid',default='')
	parser.add_argument('-r','--run',help='run script file',dest='run',action='store_true')
	parser.add_argument('-c','--continue',help='continue unfinish job in each shell',dest='continues',action='store_true')
	#parser.add_argument('-n','--nohup',help='qsub or nohup mission',dest='nohup',action='store_true')
	parser.add_argument('-quota','--quota',help='disk quota ',dest='quota',default = '1000G')
	parser.add_argument('-a','--add',help='add sequencing sample process, True -- only run added sequence sample ,False* -- run all samples',dest='add',action='store_true')
	args=parser.parse_args()

	OUTDIR = parseConfig.getab(args.outdir)
	BIN=os.path.realpath(args.bin)
	LOGFILE = '{0}/log.txt'.format(OUTDIR)
	
	job_not_continue = ' -nc '
	if args.continues : job_not_continue = ' '

	if args.jobid == '' : args.jobid = args.name
	config ,para, db , orders  = parseConfig.ReadConfig(args.input)
	shell_dir = '{0}/shell/'.format(OUTDIR)
	parseConfig.makedir(shell_dir)
	logfile = '{0}/log.txt'.format(shell_dir)
	finish_obj = JobGuard.ReadLog(logfile)
	log = open(logfile,'a')
	log.write('#pipeline version : {0}\\n'.format(BIN))
	guard_script = '{0}/guard.py'.format(shell_dir)
	job_list = {} 
'''

	for order in sorted(jobs):
		for count , a_job in  enumerate(jobs[order]):
			index = '{0}_{1}'.format(order , count)
			if a_job.Part == '':
				script += '''
	shsh = '{{0}}/{0}_{1.name}.sh'.format(shell_dir)
	with open(shsh , 'w') as f_out:
		cmds = []
		cpu = parseConfig.cpu(args.thread , 1 , '{1.CPU}' )
		queue = parseConfig.queue(args.queue , '{1.Queue}')
		cmds.append('{2[1]}'.format( para = para, OUTDIR = OUTDIR , BIN = BIN , db = db , LOGFILE = LOGFILE ))
		f_out.write('\\n'.join(set(cmds)))
	if not {1.Qsub} :
		if "{1.Node}" == '':
			a_cmd = 'perl {{1}}/src/multi-process.pl -cpu {{2}} --lines {2[0]} {{0}}'.format(shsh , bindir, cpu)
		else :
			a_cmd = 'ssh {1.Node} 2> /dev/null "perl {{1}}/src/multi-process.pl -cpu {{2}} --lines {2[0]} {{0}}"'.format(shsh , bindir, cpu)
	else:
		a_cmd = '/annoroad/share/software/install/Python-3.3.2/bin/python3 {{1}}/src/qsub_sge.py --resource "p={1.Thread} -l vf={1.Memory}" --maxjob {{2}}  --lines {2[0]} --jobprefix {{3}}{1.name}  {{5}} --queue {{4}} {{0}}'.format(shsh , bindir, cpu , args.jobid , queue , job_not_continue)
	a_thread = JobGuard.MyThread('{0}_{1.name}' , log , a_cmd, {1.Major})
	if not int({3}) in job_list: job_list[int({3})] = []
	job_list[int({3})].append(a_thread)
'''.format(index ,  a_job , a_job.format_command() , order)
			else:
				#print(a_job.name)
				script += '''
	run_sample = config['{1.Part}']
	if args.add :
		run_sample = parseConfig.chooseSamples(run_sample, orders['{1.Part}'])
	#print(run_sample)
	if len(run_sample) > 0 :
		shsh = '{{0}}/{0}_{1.name}.sh'.format(shell_dir)
		with open(shsh, 'w') as f_out:
			cmds = []
			cpu = parseConfig.cpu(args.thread , len(run_sample), '{1.CPU}' )
			queue = parseConfig.queue(args.queue , '{1.Queue}')
			for {1.Part} in run_sample:
				cmds.append('{2[1]}'.format(para=para , {1.Part} ={1.Part} ,OUTDIR=OUTDIR, BIN=BIN,db=db,LOGFILE=LOGFILE) )
			f_out.write("\\n".join(set(cmds)))
		if not {1.Qsub} :
			if "{1.Node}" == '':
				a_cmd = 'perl {{1}}/src/multi-process.pl -cpu {{2}} --lines {2[0]} {{0}}'.format(shsh , bindir, cpu)
			else:
				a_cmd = 'ssh {1.Node} 2> /dev/null "perl {{1}}/src/multi-process.pl -cpu {{2}} --lines {2[0]} {{0}}"'.format(shsh , bindir, cpu)
		else:
			a_cmd = '/annoroad/share/software/install/Python-3.3.2/bin/python3  {{1}}/src/qsub_sge.py --resource "p={1.Thread} -l vf={1.Memory}" --maxjob {{2}} --lines {2[0]} --maxcycle 5 --quota {{6}}  --jobprefix {{3}}{1.name}  {{5}} --queue {{4}} {{0}}'.format(shsh ,bindir, cpu, args.jobid , queue , job_not_continue , args.quota)
		a_thread = JobGuard.MyThread('{0}_{1.name}' , log , a_cmd, {1.Major})
		if not int({3}) in job_list: job_list[int({3})] = []
		job_list[int({3})].append(a_thread)
	else:
		print("{{0}} is empty".format("config['{1.Part}']"))
'''.format(index ,  a_job , a_job.format_command(), order)

	script +='''
	home_dir = os.environ['HOME']
	parseConfig.makedir('{0}/.mission'.format(home_dir))
	if not os.path.isfile('{0}/.mission/.pipeline.log'.format(home_dir)):
		os.system('touch {0}/.mission/.pipeline.log'.format(home_dir))
	
	tag = 0 
	with open('{0}/.mission/.pipeline.log'.format(home_dir),'r') as super_log:
		for line in super_log:
			if line.startswith('#') or re.search(pat1,line):continue
			tmp = line.rstrip().split()
			if tmp[0] == args.name:
				tag = 1
				if tmp[1] == os.path.abspath(args.outdir):
					tag = 2
	
	if tag == 0 : 
		with open('{0}/.mission/.pipeline.log'.format(home_dir),'a') as super_log:
			super_log.write('{0}\\t{1}\\n'.format(args.name , os.path.abspath(args.outdir)))
	elif tag == 2 :
		print("\033[1;31;40m" + "Warings: {0} was existed already in your log file, please check it".format(args.name) + "\033[0m")
	elif tag == 1 :
		print("\033[1;31;40m" + "Warings: {0} was existed already in your log file,  and have different analysis directory , we should add this new dir at the end of log file ,please check it".format(args.name) + "\033[0m")
		with open('{0}/.mission/.pipeline.log'.format(home_dir),'a') as super_log:
			super_log.write('{0}\\t{1}\\n'.format(args.name , os.path.abspath(args.outdir)))
	
	job_list = JobGuard.RemoveFinish(job_list,finish_obj)
	if args.run == True:
		JobGuard.run(job_list)

if __name__ == '__main__':
	main()
'''
	f_out.write(script)

def output_config(jobs, f_para):
	region = []
	paras = []
	dbs = []
	#db_part = {}
	#para_part = {}
	dbs_output = ''
	paras_output = ''
	#print(jobs)
	for i in sorted(jobs):
		for a_job in jobs[i]:
			dbs_output += '#{0.name}\n'.format(a_job)
			paras_output += '#{0.name}\n'.format(a_job)
			a_region , para ,db  = a_job.output_info()
			if not a_region == '': region.append(a_region)
			#paras += para
			for a_db in db :
				if not a_db in dbs :
					dbs_output += '{0}=\n'.format(a_db)
					dbs.append(a_db)
			for a_para in para:
				if not a_para in paras:
					paras_output += '{0}=\n'.format(a_para)
					paras.append(a_para)
#			dbs += db
	for i in list(set(region)):
		f_para.write('[{0}]\n'.format(i))
	f_para.write('[Para]\n')
	#for i in list(set(paras)):
	#	f_para.write('{0}=\n'.format(i))
	f_para.write(paras_output)
	f_para.write('[DB]\n')
	#for i in list(set(dbs)):
	#	f_para.write('{0}=\n'.format(i))
	f_para.write(dbs_output)

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input file',dest='input',type=open,required=True)
	#parser.add_argument('-c','--config',help='config example file',dest='config',type=open,required=True)
	parser.add_argument('-o','--outdir',help='outdir',dest='outdir',required=True)
	#parser.add_argument('-p','--para',help='output para file',dest='para',type=argparse.FileType('w'),required=True)
	args=parser.parse_args()

	jobs = ReadJob(args.input)
	parseConfig.makedir(args.outdir)
	with open('{0}/pipeline.py'.format(args.outdir),'w') as f_output:
		output(jobs , f_output)
	with open('{0}/config_example.txt'.format(args.outdir),'w') as f_output:
		output_config(jobs , f_output) 
	os.popen('cp -r {0}/lib {0}/src {1}'.format(bindir,args.outdir))

if __name__ == '__main__':
	main()
