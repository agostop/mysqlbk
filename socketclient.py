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
RECORD_FILE='%s%s' % (BACKPATH,'success_Send.file')
TIMESTR= time.strftime('%Y%m%d',time.localtime(time.time()))

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
	Expire_day = float(cur_time) - 60*60*24*30*3  # 3 month
	debug_log('Expire time is %s' % Expire_day)
	to_remove=[]
	record_file = RECORD_FILE.split('\\')[-1]
	debug_log('record_file is %s' % record_file)

	for dir_path,subpaths,files in os.walk(BACKPATH):
		files.remove(record_file)
		for f in files:
			_mtime=os.path.getmtime(os.path.join(dir_path,f))
			debug_log('the file name is : %s , the mtime is : %s' % (f,_mtime) )
			if _mtime < Expire_day:
				debug_log('expired is :%s' % f )
				to_remove.append(os.path.join(dir_path,f))
	
	for f in to_remove:
		os.remove(f)

def unsend_file():
	f_list=[]
	if not os.path.exists(RECORD_FILE):
		return f_list

	rfile=open(RECORD_FILE,'r')
	while 1:
		line = rfile.readline().strip()
		if not line: break
		f_list.append(line)

	ned2send_file_path = []
	for dir_path,subpaths,files in os.walk(BACKPATH):
		for f in files:
			if f == RECORD_FILE.split('\\')[-1] : continue
			if f not in f_list:
				f_path=os.path.join(dir_path,f)
				ned2send_file_path.append(f_path)

	return ned2send_file_path

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
	ready_tosend_fileinfo = {\
			'filename':filename,\
			'filesize':filesize,\
			'filecrc32':filecrc32\
			}

	data_tosend = json.dumps(ready_tosend_fileinfo)
	#data_tosend = json.dumps(fileinfo)
	debug_log('send file info data')
	tcpClient.sendall('%s' % data_tosend)

	try:
		try_num=5
		while try_num > 0 :
			ready = tcpClient.recv(BUFSIZE).strip()
			debug_log('rc is |%s|' % ready)
			if ready == 'COME_ON':
				rt=sendfiledata(filepath,filesize,tcpClient)
				if rt:
					print 'send finished'
					record_success_file(filepath)		
					break
				else:
					print 'send failed, retrans file...server response: %s, try = %d'\
							% (rt,try_num)
					try_num-=1
					#tcpClient.sendall('resend')
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
		os._exit(1)

def get_fileinfo(fpath):
	fileinfo={}
	fname = os.path.basename(fpath)
	fsize = os.stat(fpath).st_size
	crc32val = file_crc32(fpath)
	fileinfo['filename'] = fname
	fileinfo['filepath'] = fpath
	fileinfo['filesize'] = fsize
	fileinfo['filecrc32'] = crc32val
	return fileinfo

if __name__ == '__main__':
	while 1:
		time.sleep(1) 
		cur_hour=time.strftime('%H%M',time.localtime(time.time()))
		if cur_hour=='1735':
			nedsend = []
		#	new_sql_file = 'D:\\WLMP\\back_database\\20150805.zip'
			new_sql_file = sqlbak()

			nedsend = unsend_file()
			nedsend.append(new_sql_file)
			debug_log('%s'%nedsend)

			for fpath in nedsend:
				Sendfile(get_fileinfo(fpath))

			print 'now start to remove Expired file'
			rm_Expired_file()
			time.sleep(60)

