#!python
#coding=utf-8
from SocketServer import (ThreadingTCPServer,BaseRequestHandler as BRH)
import sys
import os
import json
import socket
import errno
import binascii
import time
import zlib

DEBUG=1
BUFSIZE=1024
Expire = 60*60*24*30*3 # 3 month
TMP_PATH='d:\\TMP_E\\TMP\\'
SQL_BAK='d:\\TMP_E\\sql_bak\\'

"""
@api {function} 添加窗口标题
@apiGroup Main
@apiName add_title
@apiParam None
"""
def add_title():
	if os.name == 'nt':
		import ctypes
		ctypes.windll.kernel32.SetConsoleTitleW(u'Backup Server running...')

"""
@api {class} SocketServer主函数类
@apiName MyRequestHandle
@apiGroup MyRequestHandler
@apiParam {class} BRH BaseRequestHandler基础请求句柄
"""
class MyRequestHandler(BRH):
	def debug_log(self,msg):
		if DEBUG:
			print msg
		else:
			pass
	
"""
@api {function} 对数据进行压缩
@apiDescription 使用zlib压缩将要发送的数据
@apiName decompress_data
@apiGroup MyRequestHandler
@apiParam {string} data_str 准备发送到客户端的数据
@apiSuccessExample Success-Return:
返回string类型，压缩后的数据
data

@apiErrorExample Error-Return:
返回string类型，错误信息
'error data'
"""
	def decompress_data(self,data_str):
		try:
			data=zlib.decompress(data_str)
			return data
		except:
			return 'error data'

"""
@api {function} 删除过期文件
@apiDescription 每三个月进行一次遍历，根据当前时间进行对比，超过时间的文件会被删除
@apiName rm_Expired_file
@apiGroup MyRequestHandler
@apiParam {string} FILEPATH 接收文件的文件夹路径

"""
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

"""
@api {function} 校验数据完整性
@apiDescription 校验文件的完整性，返回一个8位的校验值，recv_file函数会和客户端发送来的校验值进行对比
@apiName to_crc32
@apiGroup MyRequestHandler
@apiParam {string} filename 需要校验的文件名，具体到路径
@apiSuccessExample Success-Return:
返回string类型
8位校验值
d23nd92n

@apiErrorExample Error-Return:
返回int类型，并输出错误信息
return: 0
output:"compute file crc failed!"
"""
	def to_crc32(self,filename):
		try:
			blocksize = 1024 * 64
			f = open(filename, "rb")
			str = f.read(blocksize)
			crc = 0
			while len(str) != 0:
				crc = binascii.crc32(str,crc) & 0xffffffff #is to get unsigned int value . the crc may to like -132423  if not to this
				str = f.read(blocksize)
			f.close()

		except:
			print "compute file crc failed!"
			return 0

		return '%08x' % crc

"""
@api {function} 将临时目录中的文件移动到相应文件夹
@apiDescription 主要是根据该文件的身份信息，将该文件重新命名，并放入相应身份信息命名的文件夹
@apiName movefile
@apiGroup MyRequestHandler
@apiParam {string} tmp 临时目录路径
@apiParam {string} fin 相应归档的目录路径
@apiParam {string} fname 归档的文件名
@apiParam {string} ident 该文件的身份信息
@apiSuccessExample Success-Return:
无返回值
"""
	def movefile(self,tmp,fin,fname,ident):
		finish_dir='%s_%s'%(fin,ident)
		tmp_path='%s%s' % (tmp,fname)
		fin_path='%s\\%s' % (finish_dir,fname)
		self.debug_log('the dir info is :%s\n%s\n%s'%(finish_dir,tmp_path,fin_path))

		if not os.path.exists(finish_dir) or not os.path.isdir(finish_dir):
			os.mkdir(finish_dir)
		elif os.path.exists(fin_path):
			os.remove(fin_path)
		os.rename(tmp_path,fin_path)

"""
@api {function} 接收文件
@apiDescription 将客户端发送的文件接受到一个临时文件中，完成后返回给归档函数进行归档，接收时，如果出错会返回0，返回0时会通知客户端重试，重试5次以后会返回2，表示已达到最大重试次数，结束本次回话
@apiName recv_File
@apiGroup MyRequestHandler
@apiParam {string} tmp_file 接收时的临时文件名
@apiParam {string} file_size 接收到的通告中的文件大小信息
@apiParam {string} file_crc32 接收到的通告中的文件crc32校验值
@apiSuccessExample Success-Return:
返回int类型
return: 1

@apiErrorExample Error-Return:
返回int类型，并输出错误信息
return: 0或者2
0代表接收时出现问题，返回0后会通知客户端重试
2代表重试次数已经超过最大值
"""
	def recv_File(self, tmp_file, file_size, file_crc32):
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
				os.remove(tmp_file)
			print "close socket , have a error."
			return 0
		myfile.close()

		print 'recv over, next to check file crc32...'
		tmp_crc=self.to_crc32(tmp_file)
		self.debug_log('the tmp_crc is : %s' % tmp_crc)
		if tmp_crc == file_crc32:
			self.request.send("SUCCESS")
			print '====recv success===='
			print 'process success .'
			return 1
		else:
			if os.path.exists(tmp_file):
				os.remove(tmp_file)
			print 'recv failed, the file is incomplete'
			self.request.send("RETRY")
			data=self.request.recv(BUFSIZE)
			if data == 'TRY_AGAIN':
				return 0
			elif data == 'MAX_FAILED':
				return 2

"""
@api {function} 防扫描策略
@apiDescription 用来ban掉一些恶意扫描的ip，对客户端发送来的第一个包进行解析，如果解析失败，则加入防火墙的deny策略中
@apiName banip
@apiGroup MyRequestHandler
@apiParam {string} address 需要拒绝的IP地址
@apiSuccessExample Success-Return:
无返回值
"""
	def banip(self,address):
		blackfile = 'blacklist.txt'
		Black_ip_list = []
		if os.path.exists(blackfile):
			ip_list = file(blackfile).readlines()
			for entry in ip_list:
				Black_ip_list.append(entry.strip())
		Black_ip_list.append(address)
		to_ban = ''
		if len(Black_ip_list) > 1:
			to_ban = ','.join(Black_ip_list)
		else:
			to_ban = Black_ip_list.pop()

		firewall_cmd = 'netsh advfirewall firewall set rule name="ban" dir=in new remoteip=%s action=block' % to_ban
		os.system(firewall_cmd)

		bfile = file(blackfile,'a')
		bfile.write('%s\n'%address)	
		bfile.close()

"""
@api {function} 主句柄执行函数
@apiDescription 用来和客户端建立连接，判断消息类型，根据不同消息调用其他函数
@apiName handle
@apiGroup MyRequestHandler
@apiParam {none} none 无参数
@apiSuccessExample Success-Return:
无返回值
"""
	def handle(self):
		addr=self.client_address
		print "connected from ", addr
		data=self.request.recv(BUFSIZE)
		data=self.decompress_data(data)
		#print data

		try:
			flist=json.loads(data)
		except:
			ban_ip(self.client_address)
			print 'error data format'
			return 1

		identity=flist.pop(0)

		for fileinfo in flist:
			self.debug_log('to move file')
			filename='%s_%s' % (addr[0],fileinfo['filename'])
			file_size=fileinfo['filesize']
			file_size=int(file_size)
			file_crc32=fileinfo['filecrc32']
			tmp_path=TMP_PATH
			fin_path='%s%s' % (SQL_BAK,addr[0])
			tmp_file='%s%s' % (tmp_path,filename)

			self.debug_log('fileinfo is :\n\
					filename %s\n\
					filesize %d\n\
					filecrc32 %s'\
					% (filename,file_size,file_crc32))

			try :
				while True:
					rt = self.recv_File(tmp_file,file_size,file_crc32)
					if rt == 1:
						print 'Recv OK'
						self.movefile(tmp_path,fin_path,filename,identity)
						self.rm_Expired_file(fin_path)
						break
					elif rt == 2:
						print 'Recv failed, and max retry.'
						break
					else:
						pass

			except :
				if os.path.exists(tmp_file):
					os.remove(tmp_file)

	def setup(self):
		self.debug_log('into thread')
		self.request.settimeout(60)
	def finish(self):
		self.request.close()
		self.debug_log("exit the thread")

"""
@api {function} 封装多线程服务类
@apiGroup BackupServer
@apiName BackupServer
@apiDescription 主要是确保了安全性，处理了一些错误，摘自goagent
@apiParam {none} none 无参数
@apiSuccessExample Success-Return:
无返回值
"""
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
	add_title()
	tcpSer=BackupServer(("",10001),MyRequestHandler)
	print "waiting for connection"
	try :
		tcpSer.serve_forever()
	except KeyboardInterrupt:
		print 'over'
