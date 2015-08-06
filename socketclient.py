#!python
#coding=utf-8
from socket import *
import json,os,time,zipfile,binascii,sys,struct

DEBUG=1
BUFSIZE=1024
BAKSERV_IP = "127.0.0.1"
PORT = 10001
DATABASE_NAME='skynew'
BACKPATH='d:\\WLMP\\back_database\\'
MYSQLDUMP='d:\\WLMP\\MySQL\\bin\\mysqlbak.exe'
BACKUP_TIME='0100'
#time format "%Y%m%d%H%M%S"
RECORD_FILE='%s%s' % (BACKPATH,'success_Send.file')
TIMESTR= time.strftime('%Y%m%d',time.localtime(time.time()))
Expire=60*60*24*30*3 # 3 month

def debug_log(msg):
	if DEBUG:
		print msg
	else:
		pass

def record_success_file(filename):
	basename=os.path.basename(filename)
	try:
		rfile=open(RECORD_FILE,'a')
		rfile.write('%s\n' % basename)
	except:
		print 'the file is open or write failed'
	rfile.close()

def rm_Expired_file():
	cur_time = time.time()
	Expire_day = float(cur_time) - Expire
	debug_log('Expire time is %s' % Expire_day)
	to_remove=[]
	
	record_file = RECORD_FILE.split('\\')[-1]
	debug_log('record_file is %s' % record_file)

	for dir_path,subpaths,files in os.walk(BACKPATH):
		if os.path.exists(RECORD_FILE):
			files.remove(record_file)
		for f in files:
			_mtime=os.path.getmtime(os.path.join(dir_path,f))
			debug_log('the file name is : %s , the mtime is : %s' % (f,_mtime) )
			if _mtime < Expire_day:
				debug_log('expired is :%s' % f )
				to_remove.append(os.path.join(dir_path,f))
	
	for f in to_remove:
		os.remove(f)


def file_crc32(filename):
	try:
		blocksize = 1024 * 64
		f = open(filename, "rb")
		str = f.read(blocksize)
		crc = 0
		while len(str) != 0:
			crc = binascii.crc32(str,crc) & 0xffffffff  #is to get unsigned int value . the crc my to like -132423  if not to this
			str = f.read(blocksize)
		f.close()

	except:
		print "compute file crc failed!"
		return 0

	return '%08x' % crc

def sendfiledata(filepath,filesize,tcpClient):
	file_size = filesize
	debug_log('now open the file')
	mydata = open(filepath, "rb")
	sendsize = 0
	Flag = True
	debug_log('now send file : %s' % os.path.basename(filepath))
	while Flag:
		if file_size < sendsize+BUFSIZE:
			#debug_log('send remain data')
			chunk = mydata.read(file_size - sendsize)
			Flag = False
		else:
			sys.stdout.write('filesize is : %d \t sending %d \r' % (file_size,sendsize))
			sys.stdout.flush()
			chunk = mydata.read(BUFSIZE)
			sendsize+=BUFSIZE
		tcpClient.sendall(chunk)

	debug_log('data send over')
	mydata.close()
	
	debug_log('wait server response')
	recv=tcpClient.recv(BUFSIZE)
	debug_log('recv the message : %s' % recv)
	if recv == 'success':
		return 1
	else:
		return 0

def con_server():
	timeout=6
	tcpClient=None
	try:
		# create a ipv4 socket object
		tcpClient = socket(AF_INET)
		# set reuseaddr option to avoid 10048 socket error
		tcpClient.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		# set struct linger{l_onoff=1,l_linger=0} to avoid 10048 socket error
		tcpClient.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
		# resize socket recv buffer 8K->32K to improve browser releated application performance
		tcpClient.setsockopt(SOL_SOCKET, SO_RCVBUF, 32*1024)
		# disable negal algorithm to send http request quickly.
		tcpClient.setsockopt(SOL_TCP, TCP_NODELAY, True)
		# set a short timeout to trigger timeout retry more quickly.
		tcpClient.settimeout(timeout)
	except (error, OSError) :
		if tcpClient:
			tcpClient.close()
		return 0

	max_retry = 5
	for i in xrange(max_retry):
		try:
			tcpClient.connect((BAKSERV_IP,PORT))
			return tcpClient
		except:
			time.sleep(30)

	return 0

def Sendfile(fileinfo):
	
	tcpClient=con_server()
	if tcpClient:
		pass
	else:
		return 0

	debug_log('connect to server...')

	ready_data = []
	for fdict in fileinfo:
		_tmp = {}
		_tmp = {'filename':fdict['filename'] ,\
						'filesize':fdict['filesize'] ,\
						'filecrc32':fdict['filecrc32']}
		ready_data.append(_tmp)

	debug_log('send file info data : %s' % ready_data)
	data_tosend = json.dumps(ready_data)
	tcpClient.sendall('%s' % data_tosend)

	try:
		for fdict in fileinfo:
			filepath = fdict['filepath']
			debug_log('%s'%filepath)
			max_retry=5
			for i in xrange(max_retry):
				ready = tcpClient.recv(BUFSIZE).strip()
				debug_log('rc is |%s|' % ready)
				if ready == 'COME_ON':
					rt=sendfiledata(filepath,fdict['filesize'],tcpClient)
					if rt:
						print 'send finished'
						record_success_file(filepath)		
						break
					else:
						print 'send failed, retrans file...try %d' % i
				else:
					print "server not ready to receive, server reponse : %s" % ready
					break
		return 1
	except Exception,ex:
		print Exception,":",ex
		tcpClient.close()

def sqlbak():
	if not os.path.exists(BACKPATH):
		os.mkdir(BACKPATH)

	backfile = '%s%s.sql' % (BACKPATH,TIMESTR)
	zip_file_name = '%s%s.zip' % (BACKPATH,TIMESTR)
	debug_log('at %s backup the %s ...' % (TIMESTR,DATABASE_NAME))

	if not os.path.isfile(MYSQLDUMP):
		print 'the mysqlbak.exe is not in d:\\wlmp\\mysql\\bin'
		return 0

	sql_comm = '%s --default-character-set=utf8 -hlocalhost -R --triggers -B %s > %s' % (MYSQLDUMP,DATABASE_NAME,backfile)
	if os.system(sql_comm) == 0:
		debug_log('NOTE: %s is backup successfully' % DATABASE_NAME)
	else:
		debug_log('ERROR: %s is backup Failed, sql_comm is exec failed.' % DATABASE_NAME)

	if os.path.exists(backfile):
		f = zipfile.ZipFile(zip_file_name,'w',zipfile.ZIP_DEFLATED)
		f.write(backfile)
		os.remove(backfile)
		return zip_file_name
	else:
		debug_log('ERROR: cannot find the sql back file.')
		return 0

def get_fileinfo(allfile):
	fileinfo=[]
	for fpath in allfile:
		_info={}
		debug_log('the get_fileinfo fun , fpath is %s'%fpath)
		fname = os.path.basename(fpath)
		fsize = os.stat(fpath).st_size
		crc32val = file_crc32(fpath)
		_info['filename'] = fname
		_info['filepath'] = fpath
		_info['filesize'] = fsize
		_info['filecrc32'] = crc32val
		fileinfo.append(_info)

	return fileinfo

def unsend_file():
	f_list=[]
	if not os.path.exists(RECORD_FILE):
		return f_list

	rfile=open(RECORD_FILE,'r')
	while 1:
		line = rfile.readline().strip()
		if not line: break
		f_list.append(line)

	if not f_list:
		return f_list

	ned2send_file_path = []
	for dir_path,subpaths,files in os.walk(BACKPATH):
		for f in files:
			if f == RECORD_FILE.split('\\')[-1] : continue
			if f not in f_list:
				f_path=os.path.join(dir_path,f)
				ned2send_file_path.append(f_path)

	return ned2send_file_path

if __name__ == '__main__':
	if os.name == 'nt':
		import ctypes
		ctypes.windll.kernel32.SetConsoleTitleW(u'Backup Process running...')

	print 'backup is start up:\n\
Server IP : %s\n\
PORT : %s\n\
DATABASE_NAME : %s\n\
BACKPATH : %s\n\
MYSQLDUMP : %s\n\
BACKUP_TIME : %s'\
% (BAKSERV_IP,PORT,DATABASE_NAME,BACKPATH,MYSQLDUMP,BACKUP_TIME)
	while 1:
		time.sleep(1)
		cur_hour=time.strftime('%H%M',time.localtime(time.time()))
		#cur_hour=time.strftime('%H%M',time.localtime(1438534806))
		if cur_hour==BACKUP_TIME:
			nedsend = []
		#	new_sql_file = 'D:\\WLMP\\back_database\\20150805.zip'
			new_sql_file = sqlbak()

			nedsend = unsend_file()
			debug_log('%s'%nedsend)
			if new_sql_file :
				if new_sql_file not in nedsend:
					nedsend.append(new_sql_file)

			if nedsend:
				nedsend = get_fileinfo(nedsend)
				debug_log('%s'%nedsend)
				if not Sendfile(nedsend):
					print 'connect to server is failed!'

			print 'now start to remove Expired file'
			rm_Expired_file()
			time.sleep(60)

