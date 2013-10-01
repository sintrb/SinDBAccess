# -*- coding: UTF-8 -*
'''
Created on 2013-10-1
Test case for SinDBAccess
@author: RobinTang
'''
import MySQLdb
import sqlite3
import unittest
import time

from SinDBAccess import SinDBAccess

class SinDBAccessTest(unittest.TestCase):
	'''
	Test Case of SinDBAccess
	'''
	def __testDBA__(self, db, name):
		dba = SinDBAccess(db)
		table = 'tb_testtable'
		dba.create_table(table, {
								'name':'char(32)',
								'age':0
								}, new=True)
		stm = time.time()
		for i in range(1024):
			dba.add_object(table, {
								'name':'trb',
								'age':i
								})
		print '%s cast time: %ss'%(name, time.time() - stm)
		
	def testMySQL(self):
		'''
		Test for MYSQL
		'''
		db = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
		self.__testDBA__(db, 'testMySQL')
		
	def testSQLite(self):
		'''
		Test for sqlite
		'''
		db = sqlite3.connect("abc.db")
		self.__testDBA__(db, 'testSQLite')
		
if __name__ == '__main__':
	unittest.main()

