#! /usr/bin/env python3
import argparse
import sys
import os
import re
import sqlite3

conn = sqlite3.connect('example.db')
print("connect successfully")

c = conn.cursor()

c.execute('''CREATE TABLE filter (Sample  text PRIMARY KEY NOT NULL , Raw_Reads_Number int);''')
conn.execute("INSERT INTO filter VALUES('Y3','128979244') ; ")
#c.execute('''CREATE TABLE stocks
#		             (date text, trans text, symbol text, qty real, price real)''')

# Insert a row of data
#c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

conn.commit()
conn.close()

bindir = os.path.abspath(os.path.dirname(__file__))

__author__='Liu Tao'
__mail__= 'taoliu@annoroad.com'

pat1=re.compile('^\s+$')

def main():
	parser=argparse.ArgumentParser(description=__doc__,
			formatter_class=argparse.RawDescriptionHelpFormatter,
			epilog='author:\t{0}\nmail:\t{1}'.format(__author__,__mail__))
	parser.add_argument('-i','--input',help='input file',dest='input',type=open,required=True)
	parser.add_argument('-o','--output',help='output file',dest='output',type=argparse.FileType('w'),required=True)
	args=parser.parse_args()

	for line in args.input:
		if line.startswith('#') or re.search(pat1,line):continue
		tmp=line.rstrip().split('\t')

if __name__ == '__main__':
	main()
