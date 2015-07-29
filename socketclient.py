#!python
#coding=utf-8
from socket import *
import json,os,time,zipfile,binascii,sys,sched

DEBUG=1
BUFSIZE=1024
BAKSERV_IP = "127.0.0.1"
PORT = 10001
DATABASE_NAME='skynew'
BACKPATH='d:\\WLMP\\back_database\\'
MYSQLDUMP='d:\\WLMP\\MySQL\\bin\\mysqlbak.exe'
#time format "%Y%m%d%H%M%S"
REMFILE='%s%s' % (BACKPATH,'remember.file')
TIMESTR= time.strftime('%Y%m%d',time.localtime(time.time()))

def debug_log(msg):
	if DEBUG:
		print msg
	else:
		pass

def remember_success_file(filename):
	basename=os.path.basename(filename)
	try:
		rfile=open(REMFILE,'a')
		rfile.write(basename)
	except:
		print 'the file is open or write failed'
	rfile.close()

def check_fail_file():
	if not os.path.exists(REMFILE):
		return 0

	f_list=[]
	rfile=open(REMFILE,'r')
	while 1:
		line = rfile.readline()
		if not line: break
		f_list.append(line)

	file_list=[]
	for dir_path,subpaths,files in os.walk(BACKPATH):
		for f in files:
			if f not in f_list:
				f_path=os.path.join(dir_path,f)
				file_list.append(f_path)

	return file_list

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

def sendfiledata(filepath,filesize,BUFSIZE,tcpClient):
	file_size =filesize
	debug_log('now open the file')
	mydata = open(filepath, "rb")
	sendsize=0
	Flag=True
	while Flag:
		if file_size < sendsize+BUFSIZE:
			debug_log('send remain data')
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

def Sendfile(fileinfo):
	tcpClient = socket(AF_INET,SOCK_STREAM)
	e=0  
	try:  
	    tcpClient.connect((BAKSERV_IP,PORT))
	except tcpClient.settimeout,e:  
	    return 'connect timeout'  
	except e:  
	    return 'connect have a error'  

	debug_log('connect to server...')

	filepath = fileinfo['filepath']
	filesize = fileinfo['filesize']
	filename = fileinfo['filename']
	filecrc32  = fileinfo['filecrc32']
	ready_tosend_fileinfo = {'filename':filename,'filesize':filesize,'filecrc32':filecrc32}

	data_tosend = json.dumps(ready_tosend_fileinfo)
	debug_log('send file info data')
	tcpClient.sendall('%s' % data_tosend)

	try:
		while 1:
			ready = tcpClient.recv(BUFSIZE).strip()
			debug_log('rc is |%s|' % ready)
			if ready == 'COME_ON':
				rt=sendfiledata(filepath,filesize,BUFSIZE,tcpClient)
				if rt:
					print 'send finished'
					remember_success_file(filepath)		
					break
				else:
					print 'send failed, retrans file...server response: %s' % rt
					#tcpClient.sendall('resend')
			else:
				print "server reponse : %s" % ready
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

	sql_comm='%s --default-character-set=utf8 -hlocalhost -R --triggers -B %s > %s%s.sql' % (MYSQLDUMP,DATABASE_NAME,backfile)
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
		os._exit(1)
	
if __name__ == '__main__':
	while 1:
		time.sleep(1) 
		cur_hour=time.strftime('%H%M',time.localtime(time.time()))
		if cur_hour=='2108':

			fpath = 'D:\\skyclassSetup\\TeacherSetup469.exe'
		#	fpath = sqlbak()
			crc32val=file_crc32(fpath)
			debug_log('the file crc32 value is : %s' % crc32val)

			fname = os.path.basename(fpath)
			fsize = os.stat(fpath).st_size
			fileinfo = {'filepath':fpath, \
									'filename':fname, \
									'filesize':fsize, \
									'filecrc32':crc32val}
			ret=Sendfile(fileinfo)
			time.sleep(60)
