#!python
#coding=utf-8
from socket import *
import json,os,time,zipfile,sys

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

def sendfiledata(filepath,filesize,BUFSIZE,tcpClient):
	file_size =filesize
	debug_log('now open the file')
	mydata = open(filepath, "rb")
	sendsize=0
	Flag=True
	debug_log('sending data...')
	while Flag:
		if file_size < sendsize+BUFSIZE:
			debug_log('send remain data')
			chunk = mydata.read(file_size - sendsize)
			Flag = False
		else:
			#debug_log('sum is :%d\nsending %d\r' % (sendsize,file_size))
			sys.stdout.write('sum is :%d  sending %d\r' % (sendsize,file_size))
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
	#tcpClient.settimeout(5)  
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

	ready = tcpClient.recv(BUFSIZE).strip()
	debug_log('rc is |%s|' % ready)
	if ready == 'comeon':
		rt=sendfiledata(filepath,filesize,BUFSIZE,tcpClient)
		tcpClient.close()
		return 1
	else:
		return 0

def sqlbak():
	if not os.path.exists(BACKPATH):
		os.mkdir(BACKPATH)

	backfile = '%s%s.sql' % (BACKPATH,TIMESTR)
	zip_file_name = '%s%s.zip' % (BACKPATH,TIMESTR)
	debug_log('at %s backup the %s ...' % (TIMESTR,DATABASE_NAME))

	sql_comm='%s --default-character-set=utf8 -hlocalhost -R --triggers -B %s > %s' % (MYSQLDUMP,DATABASE_NAME,backfile)
	debug_log('sql_comm is :%s'%sql_comm)
	if os.system(sql_comm) == 0:
		debug_log('NOTE: %s is backup successfully' % DATABASE_NAME)
	else:
		debug_log('ERROR: %s is backup Failed, sql_comm is exec failed.' % DATABASE_NAME)
		os._exit(1)

	if os.path.exists(backfile):
		f = zipfile.ZipFile(zip_file_name,'w',zipfile.ZIP_DEFLATED)
		f.write(backfile)
		os.remove(backfile)
		return zip_file_name
	else:
		debug_log('ERROR: cannot find the sql back file.')
		os._exit(1)

if __name__ == '__main__':
	fpath = 'D:\\init_file\\mseinstall.exe'
	#fpath = sqlbak()
	debug_log('fpath is :%s'%fpath)
	if fpath:
		fname = os.path.basename(fpath)
		fsize = os.stat(fpath).st_size
		fileinfo = {'filepath':fpath, 'filename':fname, 'filesize':fsize}
		if not Sendfile(fileinfo):
			print 'the server not ready'
	else :
		os._exit(0)

