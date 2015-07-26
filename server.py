import SocketServer
from SocketServer import StreamRequestHandler
#class MyHandler(SocketServer.BaseRequestHandler):
class MyHandler(SocketServer.StreamRequestHandler):
	def handle(self):
		self.data=self.rfile.readline().strip()
		print "{} wrote".format(self.client_address[0])
		print self.data
		self.wfile.write(self.data.upper())

my_server = SocketServer.ThreadingTCPServer(("",10001),MyHandler)
my_server.serve_forever()
