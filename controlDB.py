import sqlite3

class DB:
	def __init__(self):
		try:
			self.conn = sqlite3.connect('server.db', check_same_thread = False, isolation_level=None)
			self.handle = self.conn.cursor()
			self.handle.execute('CREATE TABLE IF NOT EXISTS  serverstatus(id int AUTO_INCREMENT, ip varchar(20) PRIMARY KEY,namecode varchar(20),status int )')
			#self.handle.execute('CREATE TABLE IF NOT EXISTS  threshold(ip varchar(20) primary key,thrdvalue int(1))')
			#self.handle.execute('DELETE FROM serverstatus')
			#self.handle.execute('DELETE FROM threshold')
		except Exception,ex:
			print Exception,":",ex
	
	def __del__(self):
		try:
			self.handle.close()
			self.conn.commit()
			self.conn.close()
		except Exception,ex:
			print Exception,":",ex

	def addserv(self,ip,namecode,status=1):
		try:
			if not self.qrystat(ip):
				cmd = 'INSERT INTO serverstatus (ip, namecode, status) values (\'%s\',\'%s\',%d)' % (ip,namecode,status) 
				self.handle.execute(cmd)

		except Exception,ex:
			print Exception,":",ex
	
	def qrystat(self,ip=''):
		try:
			cmd = 'SELECT status FROM serverstatus WHERE ip = \'%s\'' % ip
			self.handle.execute(cmd)
			rt = self.handle.fetchone()
			return rt[0]
		except Exception,ex:
			print Exception,":",ex
			return None

	def changestatus(self,ip,status):
		try:
			cmd = 'UPDATE serverstatus SET status=%d WHERE ip=\'%s\'' % (status,ip)
			self.handle.execute(cmd)
		except Exception,ex:
			print Exception,":",ex

	def all_client(self):
		try:
			client_list = []
			cmd = 'SELECT ip FROM serverstatus'
			self.handle.execute(cmd)
			rt = self.handle.fetchall()

			for i in rt:
				client_list.append(i[0])

			return client_list
		except Exception,ex:
			print Exception,":",ex

	def servCount(self,ip):
		try:
			cmd = 'SELECT count(*) FROM serverstatus WHERE ip = \'%s\'' % ip
			self.handle.execute(cmd)
			rt = self.handle.fetchone()
			return rt[0]
		except Exception,ex:
			print Exception,":",ex
'''
	def add_client(self,ip,thrdvalue=0):
		try:
			cmd = 'INSERT INTO threshold (ip, thrdvalue) values (\'%s\',%d)' % (ip, thrdvalue)
			self.handle.execute(cmd)
		except Exception,ex:
			print Exception,":",ex
'''

'''	
	def update(self,ip,val):
		try:
			cmd = 'UPDATE threshold SET thrdvalue = %d WHERE ip = \'%s\'' % (val,ip)
			self.handle.execute(cmd)
		except Exception,ex:
			print Exception,":",ex
'''
	
'''
	def qry(self,ip):
		try:
			cmd = 'SELECT thrdvalue FROM threshold WHERE ip = \'%s\'' % ip
			self.handle.execute(cmd)
			rt = self.handle.fetchall()
			if len(rt) == 0:
				self.add_client(ip)
				return 0
			return rt[0][0]
		except Exception,ex:
			print Exception,":",ex
'''
'''
	def nedtoreport(self,ip):
		try:
			cmd = 'SELECT thrdvalue FROM threshold WHERE ip=\'%s\'' % ip
			self.handle.execute(cmd)
			rt = self.handle.fetchall()
			if rt[0][0] > 3:
				return 1
			else:
				return 0
		except Exception,ex:
			print Exception,":",ex
'''


