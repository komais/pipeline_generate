#! /usr/bin/env python3
'''
文件必须要有标题行和列名。
默认：每一行是一个特征，每一列是一个样品。
'''
import argparse
import sys
import os
import re
import time
import glob
bindir = os.path.abspath(os.path.dirname(__file__))

__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')

class FileObj:
	def __init__(self , source , to):
		tmp = source.split()
		self.source = tmp[0]
		if len(tmp) == 2 :
			self.choose = int(tmp[1])
		else:
			self.choose = None
		self.to = to
	def sourceSuffix(self):
		return self.source.split(r'.')[-1]
	def toSuffix(self):
		if self.to.find(r'.') > -1 :
			return self.to.split(r'.')[-1]
		elif self.type == 'picture'  :
			return 'png'
		else:
			return None
	def getTable(self , infile , record, rows,colnames,rownames, type):
		f_in = open(infile , 'r')
		header = []
		#samples = []
		rownames = []
		content_00 = ''
		for count , line in enumerate(f_in) : 
			if line.startswith('#') or re.search(pat1,line):continue
			tmp=line.rstrip('\n').split('\t')
			if type == 'row':
				if count == 0 :
					content_00 = tmp[0]
					colnames += tmp[1:]   ### colname is sample ids
					header = tmp[1:]
					for i in tmp[1:]:
						if not i in record:
							record[i] = {}
						else:
							print("Table : {0} is repeat,please check".format(i))
				else:
					if rows == 'all' or  count in rows:
						rownames.append(tmp[0])   ## rownames is items
						content = tmp[1:]
						for i, a_sample in enumerate(header):
							record[a_sample][tmp[0]] = content[i]  ## constrcut a two dimension dict ,first is a_sample , second is items
			elif type == 'column': # if sample in row
				if count == 0 :
					content_00 = tmp[0]
					if rows == 'all':
						rownames = tmp[1:]
					else:
						rownames = [tmp[i] for i in rows]
				else:
					colnames.append(tmp[0])
					if not tmp[0] in record:
						record[tmp[0]] = {}
					else:
						#pass
						print('Table : {0} is repeat,pls check'.format(tmp[0]))
						#sys.exit()
					if rows == 'all':
						for i , j in enumerate(tmp[1:]):
							record[tmp[0]][rownames[i]] = j
					else:
						for i , j in enumerate(rows):
							record[tmp[0]][rownames[i]] = tmp[j]
			else:
				print('Table : {0} is error'.format(type))
		f_in.close()
		return record , colnames ,  rownames , content_00 ## rownames is items, colname is sample name
	def run(self,cmd):
		print(cmd)
		if os.system(cmd):
			print('{0} is fail'.format(cmd))
	def getNumber(self,numbers):
		common_number = []
		if numbers == 'all' : return 'all'
		for i in numbers.split(','):
			if i.find('-') > -1 : 
				m,n = i.split('-')
				common_number += list(range(int(m) , int(n)+1))
			else:
				if i.isdigit():
					common_number += [int(i)]
				else:
					print('Table: config file is error:{0}'.format(i))
		return common_number
	def makedir(self):
		if self.to.find(r'.') > -1 : 
			dirs = os.path.dirname(self.to)
			os.system("mkdir -p {0}".format(dirs))
		else:
			os.system("mkdir -p {0}".format(self.to))
		time.sleep(1)
	def copy(self):
		mm = len(glob.glob(self.source))
		cmd = ''
		if self.to.find(r'.') > -1 :
			if mm > 1:
				cmd += 'cp {0} {1}'.format(self.source , os.path.dirname(self.to))
			else:
				cmd += 'cp {0} {1}'.format(self.source , self.to)
		else:
			cmd += 'cp {0} {1}'.format(self.source , self.to)
		return cmd
	def link(self,type):
		cmd = ''
		mm = len(glob.glob(self.source))
		if type == 1 :
			if self.to.find(r'.') > -1 :
				if mm > 1:
					for i in glob.glob(self.source):
						if cmd == '' :
							cmd += 'ln -s  {0} {1} '.format(i , os.path.dirname(self.to))
						else:
							cmd += ' && ln -s  {0} {1} '.format(i , os.path.dirname(self.to))
				else:
					cmd += 'ln -s  {0} {1}'.format(self.source , self.to)
			else:
				if mm > 1:
					for i in glob.glob(self.source):
						if cmd == '' :
							cmd += 'ln -s  {0} {1} '.format(i , self.to)
						else:
							cmd += ' && ln -s  {0} {1} '.format(i , self.to)
				else:
					cmd += 'ln -s  {0} {1}'.format(self.source , self.to)
		elif type == 2 :
			if self.to.find(r'.') > -1 :
				if mm > 1:
					for i in glob.glob(self.source):
						new_name = os.path.dirname(self.to) + os.path.basename(i)
						if cmd == '' :
							cmd += 'mv {0} {1} && ln -s  {2} {0} '.format(i , os.path.dirname(self.to) , new_name)
						else:
							cmd += ' && mv {0} {1} && ln -s  {2} {0} '.format(i , os.path.dirname(self.to), new_name)
				else:
					cmd += 'mv {0} {1} && ln -s  {1} {0}'.format(self.source , self.to )
			else:
				if mm > 1:
					for i in glob.glob(self.source):
						new_name = self.to + '/' + os.path.basename(i)
						if cmd == '' :
							cmd += 'mv {0} {1} && ln -s  {2} {0} '.format(i , self.to , new_name)
						else:
							cmd += ' && mv {0} {1} && ln -s  {2} {0} '.format(i , self.to , new_name)
				else:
					new_name = self.to + '/' + os.path.basename(self.source)
					cmd += 'mv {0} {1} && ln -s  {2} {0}'.format(self.source , self.to , new_name)
		else:
			print('ln file : {0} is error'.format(type))
		return cmd

class PictureObj(FileObj):
	def __init__(self , source , to):
		FileObj.__init__(self, source, to)
		self.type = 'picture'
		FileObj.makedir(self)
	def process(self ):
		cmd = ''
		mm = len(glob.glob(self.source))
		if mm  > 1 :
			if self.sourceSuffix() == self.toSuffix() or self.sourceSuffix() == 'png' :
				cmd = FileObj.copy(self)
			else:
				cmd = FileObj.copy(self)
				for i in glob.glob(self.source): 
					new_file = os.path.dirname(self.to) + '/' + os.path.splitext(os.path.basename(i))[0] + '.' + self.toSuffix()
					cmd += '&& convert {0} {1}'.format(i,new_file)
		elif mm == 1 :
			if self.sourceSuffix() == self.toSuffix()  or self.sourceSuffix() == 'png' :
				cmd = FileObj.copy(self)
			elif self.to.find(r'.') == -1 :
				cmd = FileObj.copy(self)
			else:
				cmd += 'cp {0} {1} && '.format(self.source , os.path.dirname(self.to))
				cmd += 'convert {0} {1}'.format(self.source , self.to)
		else:
			print('File not found : Picture {0} is empty'.format(self.source))
		FileObj.run(self , cmd)

class TableObj(FileObj):
	def __init__(self , source ,to):
		FileObj.__init__(self, source, to)
		self.type = 'table'
		FileObj.makedir(self)
	def process(self , process , type=''):
		if process[0] == 'copy':
			cmd = FileObj.copy(self)
			FileObj.run(self , cmd)
		elif process[0] == 'row' or process[0] == 'column':
			f_out = open(self.to , 'w')
			record = {}
			col_names = []
			row_names = []
			rows = 'all'
			c_00 = ''
			if len(process) == 1 :
				pass
			elif len(process) == 2 : ### row 1-9 
				rows = FileObj.getNumber(self , process[1])
			elif len(process) == 3 and ( process[2] == 'transpose' or process[2] == 't' ): ##row 1-8 t
				rows = FileObj.getNumber(self , process[1])
			else:
				print('Action Error : Table {0} {1} is not correct'.format(self.source , process))
			#print(rows)
			if self.choose == None:
				for i in sorted(glob.glob(self.source)):
					print(i)
					record, col_names,  row_names, c_00  = FileObj.getTable(self , i , record , rows , col_names , row_names, process[0])
			else:
				try:
					i =  sorted(glob.glob(self.source))[self.choose]
				except:
					print(self.source, self.choose ," file is not exist , pls check it ")
					sys.exit()
				print(i)
				record, col_names, row_names ,c_00  = FileObj.getTable(self , i , record , rows ,col_names , row_names, process[0])

			if type == "sorted": col_names = sorted(record.keys())

			if c_00 == '' : c_00 = 'Name'

			if len(process) == 3 and ( process[2] == 'transpose' or process[2] == 't' ):
				content = '{1}\t{0}\n'.format("\t".join(col_names),c_00)
				for j in row_names:
					content += j + "\t"
					for i in col_names:
						content += record[i][j]+"\t"
					content = content.rstrip('\n') + '\n'
			else :
				content = '{1}\t{0}\n'.format("\t".join(row_names),c_00)
				for i in col_names:
					content += i + "\t"
					for j in row_names:
						content += record[i][j]+"\t"
					content = content.rstrip('\n') + '\n'
			f_out.write(content)
		else:
			if len(process) > 0 :
				print('Action Error: Table {0} {1} is not defined'.format(self.source , process[0]))
			else:
				print('Action Error: Table {0} without any action'.format(self.source))
			sys.exit()

class OtherObj(FileObj):
	def __init__(self , source ,to):
		FileObj.__init__(self, source, to)
		self.type = 'other'
		FileObj.makedir(self)
	def process(self , process):
		if len(process) == 0 :
			cmd = FileObj.copy(self)
			FileObj.run(self,cmd)
		elif len(process) == 1 :
			if process[0] == 'link':
				cmd = FileObj.link(self , 1)
				FileObj.run(self , cmd)
			elif process[0] == 'copy':
				cmd = FileObj.copy(self)
				FileObj.run(self,cmd)
			else:
				print('config file is error:{0}'.format(process))
		elif len(process) == 2 :
			if process[0] == 'link' :
				if process[1] == 'reverse':
					cmd = FileObj.link(self , 2)
					FileObj.run(self,cmd)
				else:
					cmd = FileObj.link(self , 1)
					FileObj.run(self,cmd)
			elif process[0] == 'copy':
				cmd = FileObj.copy(self)
				FileObj.run(self,cmd)
			else:
				print('config file is error:{0}'.format(process[0]))

def getSuffix(source):
	file = source.split()[0]
	return file.split(r'.')[-1]

def checkType(suffix,a_list):
	if not suffix in a_list :
		return False
	else:
		return True

def parseLine(content , args):
	if content.find('INDIR') > -1 :
		content = content.replace('INDIR',os.path.abspath(args.indir))
	if content.find('OUTDIR') > -1 :
		content = content.replace('OUTDIR',os.path.abspath(args.outdir))
	return content

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--indir',help='input dir',dest='indir',default='.')
	parser.add_argument('-o','--outdir',help='output dir',dest='outdir',required=True)
	parser.add_argument('-c','--config',help='config file',dest='config',required=True,type=open)
	args=parser.parse_args()

	picture = ['png','pdf','svg','jpg','jpeg']
	table = ['xls']

	for line in args.config:
		if line.startswith('#') or re.search(pat1,line):continue
		tmp=line.rstrip('\n').split('\t')
		source = parseLine(tmp[0] , args)
		to = parseLine(tmp[1] , args)
		process = ''
		if len(tmp) == 3 : process = tmp[2].split()
		obj = ''
		if getSuffix(source) in picture:
			obj = PictureObj(source , to)
			obj.process()
		elif getSuffix(source) in table:
			obj = TableObj(source , to)
			obj.process(process)
		else:
			obj = OtherObj(source, to)
			obj.process(process)

if __name__ == '__main__':
	main()
