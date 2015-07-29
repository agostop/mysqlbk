#!python
#coding=utf-8
from SocketServer import (ThreadingTCPServer,BaseRequestHandler as BRH)
from time import ctime
import json,sys,os,socket

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

#	def delfailtrans(self,tmpfile):
#		if os.path.exists(tmpfile):
#			os.remove(tmpfile)
#		self.request.close()

	def handle(self):
		addr=self.client_address
		print "connected from ", addr
		data=self.request.recv(BUFSIZE)
		fileinfo=json.loads(data)
		filename='%s_%s' % (addr[0],fileinfo['filename'])
		filesize=fileinfo['filesize']
		file_size=int(filesize)
		self.debug_log('filesize is %d' % file_size)
		tmp_path='d:\\TMP_E\\TMP\\'
		fin_path='d:\\TMP_E\\sql_bak\\%s\\' % addr[0]
		try:
			myfile=open('%s%s' % (tmp_path,filename) , 'wb')
		except IOError:
			print 'file open failed'

		self.request.send("comeon")
		recv_size=0
		Flag=True
		self.debug_log('====begin to recv data====')
		try:
			while Flag:
				if file_size < recv_size+BUFSIZE :
					remain_size=file_size - recv_size
					self.debug_log('filesize is remain %d' % remain_size)
					data=self.request.recv(remain_size)
					if not data : break
					Flag=False
				else:
					data=self.request.recv(BUFSIZE)
					if not data : break
					recv_size+=len(data)
					#self.debug_log('file_size is %d\trev the recv_size : %d\r' % (file_size,recv_size))
					sys.stdout.write('file_size is %d\rrev the recv_size : %d\r' % (file_size,recv_size))
					sys.stdout.flush()
				myfile.write(data)
		except socket.timeout:
			if os.path.exists('%s%s' % (tmp_path,filename)):
				os.remove(tmpfile)
			print "caught socket.timeout exception"

		self.debug_log('sum recv %d' % (recv_size))
		myfile.close()
		
		self.movefile(tmp_path,fin_path,filename)
		self.request.send("recv ok")

	def setup(self):
		self.debug_log('into thread')
		self.request.settimeout(60)
	def finish(self):
		self.request.close()
		self.debug_log("exit thread")


class BackupServer(ThreadingTCPServer):
	"""Backup Server"""
	request_queue_size = 1024
	allow_reuse_address = True
	daemon_threads = True
	
	def close_request(self, request):
		try:
			request.close()
		except StandardError:
			pass
	
	def finish_request(self, request, client_address):
		try:
			self.RequestHandlerClass(request, client_address, self)
		except socket.error as e:
			if e[0] not in (errno.ECONNABORTED, errno.ECONNRESET, errno.EPIPE):
				raise
	
	def handle_error(self, *args):
		"""make ThreadingTCPServer happy"""
		exc_info = sys.exc_info()
		error = exc_info and len(exc_info) and exc_info[1]
		if isinstance(error, (socket.error)) and len(error.args) > 1 :
			exc_info = error = None
		else:
			del exc_info, error
			ThreadingTCPServer.handle_error(self, *args)

if __name__ == '__main__':
#	tcpSer=ThreadingTCPServer(("",10001),MyRequestHandler)
	tcpSer=BackupServer(("",10001),MyRequestHandler)
	print "waiting for connection"
	tcpSer.serve_forever()
