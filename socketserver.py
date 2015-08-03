#!python
#coding=utf-8
from SocketServer import (ThreadingTCPServer,BaseRequestHandler as BRH)
import json,sys,os,socket,errno,binascii,time

DEBUG=1
BUFSIZE=1024
Expire = 60*60*24*30*3 # 3 month
TMP_PATH='d:\\TMP_E\\TMP\\'
SQL_BAK='d:\\TMP_E\\sql_bak\\'

class MyRequestHandler(BRH):
	def debug_log(self,msg):
		if DEBUG:
			print msg
		else:
			pass

	def rm_Expired_file(self,FILEPATH):
		cur_time = time.time()
		Expire_day = float(cur_time) - Expire
		self.debug_log('Expire time is %s' % Expire_day)
		to_remove=[]

		for dir_path,subpaths,files in os.walk(FILEPATH):
			for f in files:
				_mtime=os.path.getmtime(os.path.join(dir_path,f))
				self.debug_log('the file name is : %s , the mtime is : %s' % (f,_mtime) )
				if _mtime < Expire_day:
					self.debug_log('expired is :%s' % f )
					to_remove.append(os.path.join(dir_path,f))
		
		for f in to_remove:
			os.remove(f)

	def to_crc32(self,filename):
		try:
			blocksize = 1024 * 64
			f = open(filename, "rb")
			str = f.read(blocksize)
			crc = 0
			while len(str) != 0:
				crc = binascii.crc32(str,crc) & 0xffffffff #is to get unsigned int value . the crc my to like -132423  if not to this
				str = f.read(blocksize)
			f.close()

		except:
			print "compute file crc failed!"
			return 0

		return '%08x' % crc

	def movefile(self,tmp,fin,fname):
		tmp_path='%s%s' % (tmp,fname)
		fin_path='%s%s' % (fin,fname)
		if not os.path.exists(fin) or not os.path.isdir(fin):
			os.mkdir(fin)
		elif os.path.exists(fin_path):
			os.remove(fin_path)
		os.rename(tmp_path,fin_path)

	def recv_File(self,tmp_file,file_size):
		try:
			myfile=open(tmp_file , 'wb')
		except IOError:
			print 'file open failed'
			self.request.send("FILE_OPEN_FAILED")
			return 0

		self.request.send("COME_ON")
		recv_size=0
		Flag=True
		self.debug_log('====begin to recv data====')
		print 'file size is : %s' % file_size
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
					sys.stdout.write('rev the recv_size : %d\r' % recv_size)
					sys.stdout.flush()
				myfile.write(data)
		except :
			myfile.close()
			if os.path.exists(tmp_file):
				os.remove(tmpfile)
			print "close socket , have a error."
			return 0
		myfile.close()
		return 1

	def handle(self):
		addr=self.client_address
		print "connected from ", addr
		data=self.request.recv(BUFSIZE)
		print data

		try:
			#fileinfo=json.loads(data)
			flist=json.loads(data)
		except:
			print 'error data format'
			return 1

		for fileinfo in flist:
			filename='%s_%s' % (addr[0],fileinfo['filename'])
			file_size=fileinfo['filesize']
			file_size=int(file_size)
			file_crc32=fileinfo['filecrc32']

			self.debug_log('fileinfo is :\n\
					filename %s\n\
					filesize %d\n\
					filecrc32 %s'\
					% (filename,file_size,file_crc32))

			tmp_path=TMP_PATH
			fin_path='%s%s\\' % (SQL_BAK,addr[0])
			tmp_file='%s%s' % (tmp_path,filename)

			while 1:
				if self.recv_File(tmp_file,file_size):
					print 'recv over, next to check file crc32...'
					tmp_crc=self.to_crc32(tmp_file)
					self.debug_log('the tmp_crc is : %s' % tmp_crc)
					if tmp_crc == file_crc32:
						self.request.send("success")
						print '====recv success===='
						self.movefile(tmp_path,fin_path,filename)
						print 'process success .'
						self.rm_Expired_file(fin_path)
						break
					else:
						os.remove(tmp_file)
						print 'recv failed, the file is incomplete'
						self.request.send("retry")
				else : break

	def setup(self):
		self.debug_log('into thread')
		self.request.settimeout(60)
	def finish(self):
		self.request.close()
		self.debug_log("exit the thread")

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
