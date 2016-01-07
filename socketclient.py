#!python
#coding=utf-8
from socket import *
import os
import sys
import time
import json
import zlib
import zipfile
import binascii
import struct
import ConfigParser

def print_info():
	print '\
--------------------------------------------\n\
backup is start up !\n\
Identity : %s\n\
Server IP : %s\n\
PORT : %s\n\
DATABASE_NAME : %s\n\
BACKPATH : %s\n\
MYSQLDUMP : %s\n\
CYCLE_TIME : %s\n\
BACKUP_TIME : %s\n\
---------------------------------------------'\
% (IDENTITY,BAKSERV_IP,PORT,DATABASE_NAME,BACKPATH,MYSQLDUMP,CYCLE_TIME,BACKUP_TIME)

def parseConfig():
	CONFIG=ConfigParser.ConfigParser()
	CONFIG_FILE_NAME=os.path.splitext(os.path.abspath(__file__))[0]+'.ini'
	if os.path.exists(CONFIG_FILE_NAME):
		CONFIG.read(CONFIG_FILE_NAME)
	else:
		print "can not found the config file"
		os._exit(1)

	global DEBUG,IDENTITY,BUFSIZE,BAKSERV_IP,PORT,DATABASE_NAME,BACKPATH,MYSQLDUMP,BACKUP_TIME,Expire,RECORD_FILE,TIME_FORMAT,CYCLE_TIME
	one_month=60*60*24*30
	DEBUG = int(CONFIG.get('client','DEBUG'))
	IDENTITY = (CONFIG.get('client','IDENTITY')).decode('utf-8')
	BUFSIZE = int(CONFIG.get('client','BUFSIZE'))
	BAKSERV_IP = CONFIG.get('client','BAKSERV_IP')
	PORT = int(CONFIG.get('client','PORT'))
	DATABASE_NAME = CONFIG.get('client','DATABASE_NAME')
	BACKPATH = CONFIG.get('client','BACKPATH')
	MYSQLDUMP = CONFIG.get('client','MYSQLDUMP')
	Expire = float(int(CONFIG.get('client','Expire')) * one_month)
	CYCLE_TIME = CONFIG.get('client','CYCLE_TIME')
	TIME_OF_DAY = CONFIG.get('client','TIME_OF_DAY')
	WEEK = CONFIG.get('client','DAY_OF_WEEK')
	MONTH = CONFIG.get('client','DAY_OF_MONTH')
	RECORD_FILE='%s%s' % (BACKPATH,'success_Send.file')

	if CYCLE_TIME.upper() == 'D':
		TIME_FORMAT = '%H%M'
		BACKUP_TIME = '%s' % TIME_OF_DAY
	elif CYCLE_TIME.upper() == 'W':
		TIME_FORMAT = '%w%H%M'
		BACKUP_TIME = '%s%s' % (WEEK,TIME_OF_DAY)
	elif CYCLE_TIME.upper() == 'M':
		TIME_FORMAT = '%d%H%M'
		BACKUP_TIME = '%s%s' % (MONTH,TIME_OF_DAY)

	print_info()

def debug_log(msg):
	if DEBUG:
		print msg
	else:
		pass

def record_success_file(filename):
	basename=os.path.basename(filename)
	#try:
	rfile=open(RECORD_FILE,'a')
	rfile.write('%s\n' % basename)
	#except:
	#	if rfile:
	#		rfile.close()
	#	print 'the file is open or write failed'
	rfile.close()

def rm_Expired_file():
	cur_time = time.time()
	debug_log('cur_time is : %s\nExpire is : %s' % (float(cur_time),float(Expire)))
	Expire_day = float(cur_time) - float(Expire)
	debug_log('Expire time is %f' % Expire_day)
	to_remove=[]

	record_file = os.path.basename(RECORD_FILE)

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

	if to_remove :
		exp_filename = []
		not_exp_file = []

		for f in to_remove:
			os.remove(f)
			exp_filename.append(os.path.basename(f))

		debug_log("exp_filename : %s " % str(exp_filename))

		rc_filename = []
		rc_file = open(RECORD_FILE,"r")
		for file_name in rc_file:
			rc_filename.append(file_name.strip())
		rc_file.close()

		debug_log("rc_filename : %s " % str(rc_filename))

		not_exp_file = list(set(rc_filename) - set(exp_filename))
		debug_log("not_exp_file : %s " % str(not_exp_file))

		update_rcfile = open(RECORD_FILE,"w")
		update_rcfile.truncate()
		for _file in not_exp_file:
			update_rcfile.write('%s\n' % _file)

		update_rcfile.close()

def compress_data(data_str):
	return zlib.compress(data_str,zlib.Z_BEST_COMPRESSION)

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
	if recv == 'SUCCESS':
		return 1
	elif recv == 'RETRY':
		return 0

def con_server():
	timeout=6
	tcpClient=None
	try:
		# create a ipv4 socket object
		tcpClient = socket(AF_INET)
		# set reuseaddr option to avoid 10048 socket error
		tcpClient.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		# set a short timeout to trigger timeout retry more quickly.
		tcpClient.settimeout(timeout)
	except (error, OSError) :
		if tcpClient:
			tcpClient.close()
		return 0

	print 'try to connect server %s' % (BAKSERV_IP)
	max_retry = 5
	for i in xrange(max_retry):
		try:
			tcpClient.connect((BAKSERV_IP,PORT))
			return tcpClient
		except:
			time.sleep(30)

	return 0

def Prepare_data(fileinfo):
	identity = IDENTITY.encode('utf-8')

	_data = [
			{
			'filename':fdict['filename'],
			'filesize':fdict['filesize'], 
			'filecrc32':fdict['filecrc32']
			}	for fdict in fileinfo
	]

	_data.insert(0,identity)

	debug_log('send file info data : %s' % _data)
	return compress_data(json.dumps(_data))

def Sendfile(fileinfo):
	debug_log('connect to server...')

	tcpClient=con_server()
	if tcpClient:
		pass
	else:
		return 0

	data_tosend = Prepare_data(fileinfo)
	tcpClient.sendall('%s' % data_tosend)

	try:
		for fdict in fileinfo:
			filepath = fdict['filepath']
			debug_log('%s'%filepath)
			max_retry=5
			#for i in xrange(max_retry):
			while True:
				ready = tcpClient.recv(BUFSIZE).strip()
				debug_log('rc is |%s|' % ready)
				if ready == 'COME_ON':
					rt=sendfiledata(filepath,fdict['filesize'],tcpClient)
					if rt == 1:
						print 'send finished'
						record_success_file(filepath)		
						break
					elif rt == 2:
						print 'the server recvie failed, server have a error.'
						break
					else:
						print 'send failed, retrans file...try %d' % max_retry
						max_retry-=1
						if max_retry == 0:
							tcpClient.sendall('MAX_FAILED')
							print 'the file trans failed , try max. '
							break
						else :
							tcpClient.sendall('TRY_AGAIN')
							
				else:
					print "server not ready to receive, server reponse : %s" % ready
					break

		return 0
	except Exception,ex:
		print Exception,":",ex
		tcpClient.close()
	except KeyboardInterrupt:
		if tcpClient:
			tcpClient.close()

def sqlbak():
	file_name_array = []
	TIMESTR= time.strftime('%Y%m%d',time.localtime(time.time()))
	if not os.path.exists(BACKPATH):
		os.mkdir(BACKPATH)

	for dname in DATABASE_NAME.split(','):
		backfile = '%s%s.sql' % (BACKPATH,TIMESTR)
		zip_file_name = '%s%s_%s.zip' % (BACKPATH,dname,TIMESTR)
		debug_log('at %s backup the %s ...' % (TIMESTR,dname))
	
		if not os.path.isfile(MYSQLDUMP):
			print 'the mysqlbak.exe is not in %s' % MYSQLDUMP
			return file_name_array
	
		sql_comm = '%s --default-character-set=utf8 -hlocalhost --single-transaction -R --triggers -B %s > %s' % (MYSQLDUMP,dname,backfile)
	
		try:
			os.system(sql_comm) # can't return correct value
		except Exception,ex:
			print Exception,":",ex
			os.exit(1)
			return file_name_array

		if os.path.exists(backfile):
			d=open(backfile)
			data=d.read()
			l=len(data)
			d.close()

			if l == 0:
				print 'ERROR: %s is backup Failed, sql_comm is exec failed.' % dname
				return file_name_array
			else:
				f = zipfile.ZipFile(zip_file_name,'w',zipfile.ZIP_DEFLATED)
				f.write(backfile)
				os.remove(backfile)
				f.close()
				file_name_array.append(zip_file_name)

		else:
			debug_log('ERROR: cannot find the sql back file.')
			return file_name_array

	return file_name_array

def get_fileinfo(allfile):
	fileinfo=	[ 
			{ 
				'filename' : os.path.basename(fpath) ,
				'filepath' : fpath,
				'filesize' : os.stat(fpath).st_size,
				'filecrc32' : file_crc32(fpath) 
			} for fpath in allfile 
	]

	debug_log('the get_fileinfo fun , fileinfo is %s'% fileinfo )

	return fileinfo

def to_send_file(new_sql_file):
	f_list=[]
	if not os.path.exists(RECORD_FILE):
		f_list+=new_sql_file
		return get_fileinfo(f_list)

	rfile=open(RECORD_FILE,'r')
	while 1:
		line = rfile.readline().strip()
		if not line: break
		f_list.append(line)

	if not f_list:
		return f_list

	debug_log(' new_sql_file : %s' % new_sql_file)
	ned2send_file_path = []
	for dir_path,subpaths,files in os.walk(BACKPATH):
		for f in files:
			if f == os.path.basename(RECORD_FILE) : continue
			if f not in f_list:
				f_path=os.path.join(dir_path,f)
				ned2send_file_path.append(f_path)

	debug_log('1 ned2send_file_path : %s'%ned2send_file_path)

	ned2send_file_path = list(set(ned2send_file_path+new_sql_file)) #去重合并

	if ned2send_file_path:
		ned2send_file_path = get_fileinfo(ned2send_file_path)
		debug_log('2 ned2send_file_path : %s'%ned2send_file_path)

	return ned2send_file_path

def add_title():
	if os.name == 'nt':
		import ctypes
		ctypes.windll.kernel32.SetConsoleTitleW(u'Backup Process running...')

def main():
	parseConfig()

	while 1:
		time.sleep(1)
		cur_hour=time.strftime(TIME_FORMAT,time.localtime(time.time()))
		#cur_hour=time.strftime('%H%M',time.localtime(1438534806))
		if cur_hour==BACKUP_TIME:

#			new_sql_file = 'D:\\WLMP\\back_database\\skynew_20150817.zip'
			new_sql_file = sqlbak()
			if len(new_sql_file) == 0:
				print 'the sqlbak command is exec failed !'

			Sendfile(to_send_file(new_sql_file))

			print 'now start to remove Expired file'
			rm_Expired_file()
			print '=============process over=============='
			time.sleep(60)

def daemonize():
	stdin = '/dev/null'
	stderr = '/dev/null'
	stdout = '/dev/null'
	try: 
		pid = os.fork() 
		if pid > 0:
			# exit first parent
	      		sys.exit(0) 
	except OSError, e: 
		sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
		sys.exit(1)
	
	# decouple from parent environment
	os.chdir(os.path.split(os.path.realpath(__file__))[0]) 
	os.setsid() 
	os.umask(0) 
	
	# do second fork
	try: 
		pid = os.fork() 
		if pid > 0:
			# exit from second parent
			sys.exit(0) 
	except OSError, e: 
		sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
		sys.exit(1) 
	
	# redirect standard file descriptors
	sys.stdout.flush()
	sys.stderr.flush()
	si = file(stdin, 'r')
	so = file(stdout, 'a+')
	se = file(stderr, 'a+', 0)
	os.dup2(si.fileno(), sys.stdin.fileno())
	os.dup2(so.fileno(), sys.stdout.fileno())
	os.dup2(se.fileno(), sys.stderr.fileno())

if __name__ == '__main__':
	try :
		if os.name != 'nt':
			daemonize()
		else:
			add_title()

		main()
	except KeyboardInterrupt:
		print 'quit'
