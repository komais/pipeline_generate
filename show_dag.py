#! /usr/bin/env python3
import argparse
import sys
import os
import re
bindir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(bindir)
import pipeline_generate


__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')

def draw_box(a_level_job , output):
	contents = [''] * 7 
	for i , a_job in enumerate(a_level_job):
		contents[0] += '-' * 25 + ' ' * 5 
		contents[1] += '|'+ '{0:^23}'.format('Name:'+a_job.name) + '|' + ' ' * 5 
		contents[2] += '|'+ '{0:^23}'.format( 'Major:'+ a_job.Major) + '|' + ' ' * 5 
		contents[3] += '|'+ '{0:^23}'.format('CPU:'+a_job.CPU) + '|' + ' ' * 5 
		contents[4] += '|'+ '{0:^23}'.format('Memory:'+ a_job.Memory) + '|' + ' ' * 5 
		contents[5] += '|'+ '{0:^23}'.format('Thread:'+ str(a_job.Thread)) + '|' + ' ' * 5
		contents[6] += '-' * 25 + ' ' * 5
	for i in contents:
		print(i)
		output.write( i + '\n')


def show_dag(jobs , output):
	for i  in jobs:
		print('level' + str(i))
		output.write('level' + str(i) + '\n')
		draw_box(jobs[i] , output)
		print()
		print()
		output.write('\n')
		output.write('\n')

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input pipeline config file',dest='input',type=open,required=True)
	parser.add_argument('-o','--output',help='output file',dest='output',type=argparse.FileType('w'),required=True)
	#parser.add_argument('-m','--mm',help='output file',dest='mm',action='store_false')
	args=parser.parse_args()
	
	jobs = pipeline_generate.ReadJob(args.input)
	show_dag(jobs , args.output)

if __name__ == '__main__':
	main()
