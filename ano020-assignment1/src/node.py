import time
import BaseHTTPServer
import sys
import getopt
import threading
import signal
import socket
import httplib
import random
import string
import hashlib
import logging

MAX_CONTENT_LENGHT = 1024		# Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600	# Maximum total storage allowed (100 megabytes)
logging.basicConfig(filename='example.log',level=logging.DEBUG)
node_port = 8009
MapMap = dict()	
httpdServeRequests = True
NodeList = ['compute-1-1', 'compute-1-2', 'compute-1-3'];
ThisNode = ''
CorrectNodes = ['compute-1-1.local', 'compute-1-2.local', 'compute-1-3.local']


class StorageServerBackEnd:
	
	def __init__(self):

	#  Dictionary which holds the key/value pairs
		self.size = 0
	
	

class BackEndHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	global backend 
	backend = StorageServerBackEnd()
	
	# Returns the 
	def do_GET(self):
		key = self.path
		
		hashedkey = abs(hash(key)) % (10 ** 8)
		hashedkey = hashedkey % 3
		
		ThisNode = socket.gethostname()

		if ThisNode == CorrectNodes[hashedkey]:

			value = MapMap[key]


		else:
			if CorrectNodes[hashedkey] == CorrectNodes[0]:
				logging.info('ONE')
				logging.info(NodeList[0])
				logging.info(hashedkey)
				value = self.getTestObject(self.path, NodeList[0])
			elif CorrectNodes[hashedkey] == CorrectNodes[1]:
				logging.info('TWO')
				logging.info(NodeList[1])
				logging.info(hashedkey)
				value = self.getTestObject(self.path, NodeList[1])
			elif CorrectNodes[hashedkey] == CorrectNodes[2]:
				logging.info('THREE')
				logging.info(NodeList[2])
				logging.info(hashedkey)
				value = self.getTestObject(self.path, NodeList[2])
			else:
				return error

		# Write header
		self.send_response(200)
		self.send_header("Content-type", "application/octet-stream")
		self.end_headers()
		
		self.wfile.write(value)
	def getTestObject(self, key, sendtothisnode):

		try:
			conn = httplib.HTTPConnection(sendtothisnode, node_port)
			conn.request("GET", key)
			response = conn.getresponse()
			if response.status != 200:
				print response.reason
				return False

				
			retrievedValue = response.read()
		except:
			print "Unable to send GET request"
			return False
		

		return retrievedValue	
		
	def do_PUT(self):
		contentLength = int(self.headers['Content-Length'])
		
		# Forward the request to the backend servers
		# Write Headers

		#backend.sendPUT(self.path, self.rfile.read(contentLength), contentLength)
		key = self.path
		value = ''
		value += self.rfile.read(contentLength)
		
		hashedkey = abs(hash(key)) % (10 ** 8)
		hashedkey = hashedkey % 3



		ThisNode = socket.gethostname()
		#logging.info(ThisNode)
	#	if ThisNode == CorrectNodes[0]:
	#		logging.info('ONE')
	#	if ThisNode == CorrectNodes[1]:
	#		logging.info('TWO')
	#	if ThisNode == CorrectNodes[2]:
	#		logging.info('THREE')
		if ThisNode == CorrectNodes[hashedkey]:
			logging.info('Found Correct Node!')
			MapMap[key] = value
			#self.wfile.write(value)
		else:
			if CorrectNodes[hashedkey] == CorrectNodes[0]:
				logging.info('ONE')
				logging.info(NodeList[0])
				logging.info(hashedkey)
				self.putTestObject(self.path, value, NodeList[0])
			elif CorrectNodes[hashedkey] == CorrectNodes[1]:
				logging.info('TWO')
				logging.info(NodeList[1])
				logging.info(hashedkey)
				self.putTestObject(self.path, value, NodeList[1])
			elif CorrectNodes[hashedkey] == CorrectNodes[2]:
				logging.info('THREE')
				logging.info(NodeList[2])
				logging.info(hashedkey)
				self.putTestObject(self.path, value, NodeList[2])
			else:
				return error

		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.end_headers()
	def sendErrorResponse(self, code, msg):
		self.send_response(code)
		self.send_header("Content-type", "text/html")
		self.end_headers()
		self.wfile.write(msg)

	
	def putTestObject(self, key, value, sendtothisnode):
		print "PUT(key, value):", key, value
		try:
			conn = httplib.HTTPConnection(sendtothisnode, node_port)
			conn.request("PUT", key, value)
			response = conn.getresponse()
			print "Response =", response 
			logging.info('RESPONSE')
		except:
			logging.info ('No response!')
			return False
		
		return True
		

class BackEndHTTPServer(BaseHTTPServer.HTTPServer):
	
	def server_bind(self):
		BaseHTTPServer.HTTPServer.server_bind(self)
		self.socket.settimeout(1)
		self.run = True


	def get_request(self):
		while self.run == True:
			try:
				sock, addr = self.socket.accept()
				sock.settimeout(None)
				logging.info('Got connection!')
				return (sock, addr)
			except socket.timeout:
				logging.info('Timed out?')
				if not self.run:
					raise socket.error

	def stop(self):
		self.run = False

	def serve(self):
		while self.run == True:
			self.handle_request()




if __name__ == '__main__':

	httpserver_port = 8009

	try:
		logging.warning('Server setup!')
		httpd = BackEndHTTPServer(("",httpserver_port), BackEndHTTPHandler)
		server_thread = threading.Thread(target = httpd.serve)
		server_thread.daemon = True
		server_thread.start()


		
		def handler(signum, frame):
			print "Stopping http server..."
			httpd.stop()
		signal.signal(signal.SIGINT, handler)
		
	except:
		print "Error: unable to http server thread"
	
	# Run a series of tests to verify the storage integrity

	# Wait for server thread to exit
	server_thread.join(100)