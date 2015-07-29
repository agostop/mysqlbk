#!python
#coding=utf-8
from socket import *
import json,os,time,zipfile,binascii,sys

DEBUG=1
BUFSIZE=1024
BAKSERV_IP = "127.0.0.1"
PORT = 10001
DATABASE_NAME='skynew'
BACKPATH='d:\\WLMP\\back_database\\'
MYSQLDUMP='d:\\WLMP\\MySQL\\bin\\mysqlbak.exe'
#time format "%Y%m%d%H%M%S"
TIMESTR= time.strftime('%Y%m%d',time.localtime(time.time()))

def debug_log(msg):
	if DEBUG:
		print msg
	else:
		pass

def file_crc32(filename):
	try:
		blocksize = 1024 * 64
		f = open(filename, "rb")
		str = f.read(blocksize)
		crc = 0
		while len(str) != 0:
			crc = binascii.crc32(str,crc) & 0xffffffff
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
			sys.stdout.('filesize is : %d \t sending %d \r' % (file_size,sendsize))
			sys.stdout.flush()
			chunk = mydata.read(BUFSIZE)
			sendsize+=BUFSIZE
		tcpClient.sendall(chunk)

	debug_log('data send over')
	mydata.close()
	
	#debug_log('now to send EOF')
	#tcpClient.sendall('EOF\n')
	debug_log('wait server response')
	recv=tcpClient.recv(BUFSIZE)
	debug_log('recv the message : %s' % recv)
	if recv:
		print recv
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
	fileinfo = {'filename':filename,'filesize':filesize}

	data_tosend = json.dumps(fileinfo)
	debug_log('send file info data')
	tcpClient.sendall('%s' % data_tosend)

	try:
		ready = tcpClient.recv(BUFSIZE).strip()
		debug_log('rc is |%s|' % ready)
		if ready == 'comeon':
			rt=sendfiledata(filepath,filesize,BUFSIZE,tcpClient)
			tcpClient.close()
		
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
	fpath = 'd:\\skyclassSetup\\LiveInfoSetup.exe'
	#fpath = sqlbak()
	debug_log(file_crc32(fpath))
#	fname = os.path.basename(fpath)
#	fsize = os.stat(fpath).st_size
#	fileinfo = {'filepath':fpath, 'filename':fname, 'filesize':fsize}
#	ret=Sendfile(fileinfo)

