#!python
#coding=utf-8
from SocketServer import (ThreadingTCPServer,BaseRequestHandler as BRH)
from time import ctime
import json,os

DEBUG=1
BUFSIZE=1024

class MyRequestHandler(BRH):
	def debug_log(self,msg):
		if DEBUG:
			print msg
		else:
			pass

	def movefile(self,tmp,fin,fname):
		tmp_path='%s%s' % (tmp,fname)
		fin_path='%s%s' % (fin,fname)
		if not os.path.exists(fin) or not os.path.isdir(fin):
			os.mkdir(fin)
		os.rename(tmp_path,fin_path)

	def handle(self):
		self.timeout=5
		add=self.client_address
		print "connected from ", add
		data=self.request.recv(BUFSIZE)
		fileinfo=json.loads(data)
		filename=fileinfo['filename']
		filesize=fileinfo['filesize']
		file_size=int(filesize)
		self.debug_log('filesize is %d' % file_size)
		tmp_path='d:\\TMP\\'
		fin_path='d:\\sql_bak\\%s\\' % add[0]
		try:
			myfile=open('%s%s' % (tmp_path,filename) , 'wb')
		except IOError:
			print 'file open failed'

		self.request.send("comeon")
		recv_size=0
		Flag=True
		data=''
		while Flag:
			if file_size < recv_size+BUFSIZE :
				remain_size=file_size - recv_size
				self.debug_log('filesize is remain %d' % remain_size)
				data=self.request.recv(remain_size)
				Flag=False
			else:
				data=self.request.recv(BUFSIZE)
				recv_size+=BUFSIZE

			myfile.write(data)
		self.debug_log('recv size is %d' % (recv_size+remain_size))
		myfile.close()
		
		self.movefile(tmp_path,fin_path,filename)

		self.request.send("recv ok")

tcpSer=ThreadingTCPServer(("",10001),MyRequestHandler)
print "waiting for connection"
tcpSer.serve_forever()
