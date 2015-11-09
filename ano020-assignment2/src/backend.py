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
import urllib2

MAX_CONTENT_LENGHT = 1024		# Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600	# Maximum total storage allowed (100 megabytes)
logging.basicConfig(filename='example.log',level=logging.DEBUG)
node_port = 8009
MapMap = dict()	
httpdServeRequests = True
NodeList = ['compute-1-1', 'compute-1-2', 'compute-1-3', 'compute-2-1', 'compute-2-2', 'compute-2-3', 'compute-3-1', 'compute-3-2', 'compute-3-3', 'compute-4-1']
#NodeList = ['compute-1-1', 'compute-1-2', 'compute-1-3']
ThisNode = ''
CorrectNodes = ['compute-1-1.local', 'compute-1-2.local', 'compute-1-3.local' , 'compute-2-1.local', 'compute-2-2.local', 'compute-2-3.local', 'compute-3-1.local', 'compute-3-2.local', 'compute-3-3.local', 'compute-4-1.local']
#CorrectNodes = ['compute-1-1.local', 'compute-1-2.local', 'compute-1-3.local']
NextNode= ''
NumNode = len(NodeList)
Node_ID = 0
LeaderNode= 15
Find_leader_hoax = 9919





class BackEndHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
	def do_GET(self):
		global LeaderNode
		global NextNode
		value = ''
		ThisNode = socket.gethostname()
		get_message = self.path.split()
### Checks every node if they're alive and well
		if get_message[0] == "/getNodes":
			value = ''
			for x in range (0, Node_ID):
				if self.check_node(x) == True:
					value += NodeList[x] + ":" + str(node_port) +"\n"
			for x in range(Node_ID+1, len(NodeList)):
				if self.check_node(x) == True:
					value += NodeList[x] + ":" + str(node_port) +"\n"
		

### For the circle development, each node points to its next rank:
			if self.check_next(NextNode) == True: #IF NEXTNODE IS ALIVE AND WELL
				#value = NextNode  + ":" +  str(node_port)
				print "function turned off for now"
			else: #NEXTNODE IS DEAD
				NextNode = 0
				N_U = 0
				N_L = 0
				for x in range(Node_ID + 1, len(NodeList)): #Iterate over remaining nodes higher than NODEID
					if self.check_node(x):
						NextNode = NodeList[x]
						N_U = NextNode
						#value = NextNode  + ":" +  str(node_port)
						break
				if N_U == 0: #No Higher nodes online
					for x in range(0, Node_ID): #ITERATE OVER LOWER NODES
						if self.check_node(x):
							NextNode = NodeList[x]
							N_L = NextNode
							#value = NextNode  + ":" +  str(node_port)
							break
				if N_U == 0 and N_L == 0:
					NextNode = NodeList[Node_ID]
					#value = NextNode  + ":" +  str(node_port)


			

		if get_message[0] == "/getCurrentLeader":
			#self.find_leader(Node_Score, ThisNode, NextNode, LeaderNode)
			if Node_ID == LeaderNode:
				value = NodeList[LeaderNode] + ":" + str(node_port)
			else:
				if self.check_node(LeaderNode): #Leader is alive and well
					value = NodeList[LeaderNode] + ":" + str(node_port)
				else: #LEADER IS DEAD
					if Node_ID == LeaderNode-1: ##next new leader made discovery
						LeaderNode = Node_ID
						value = NodeList[Node_ID] + ":" + str(node_port)
					else: #Peasants made the discovery, election time!
						num_times = len(NodeList) - 1 - Node_ID
						ret = 0
						total_ret = 0
						for x in range(Node_ID+1, len(NodeList)):
							send_message = "/ELECT_NEW_LEADER:"
							ret = self.elect_new_leader(send_message,x)
							total_ret += int(ret)
							if int(ret) < -1:
								LeaderNode = int(ret) + 9919
								value = NodeList[LeaderNode] + ":" + str(node_port)
							if int(ret) > 9000:
								LeaderNode = int(ret) - 9919
								value = NodeList[LeaderNode] + ":" + str(node_port)
								break
						if total_ret == num_times:
							value = NodeList[Node_ID] + ":" + str(node_port) + ":" + str(node_port)



		if get_message[0] == "/ELECT_NEW_LEADER:":
			num_times = len(NodeList)  - 1 - Node_ID
			value = 0
			ret = 0
			if Node_ID + 1 == len(NodeList):
				LeaderNode = Node_ID
				value = 9919 + Node_ID    
				ret = 1
			else:
				for x in range(Node_ID+1, len(NodeList)):
					send_message = "/ELECT_NEW_LEADER:"
					ret += int(self.elect_new_leader(send_message, x))
			if ret == num_times:
				LeaderNode = Node_ID
				value = 9919 + Node_ID    
				ret = 1



		self.send_response(200)
		self.send_header("Content-type", "application/octet-stream")
		self.end_headers()
		self.wfile.write(value)


	def sendErrorResponse(self, code, msg):
		self.send_response(code)
		self.send_header("Content-type", "text/html")
		self.end_headers()
		self.wfile.write(msg)


	def broadcast_new_leader(self, value, ID):
		try:
			conn = httplib.HTTPConnection(NodeList[ID], node_port)
			conn.request ("GET", value)
			response = conn.getresponse()
			print "Response =", response
		except:
			return False
		return True

	def elect_new_leader(self, value, ID):
		ret_val = 1
		try:
			conn = httplib.HTTPConnection(NodeList[ID], node_port)
			conn.request ("GET", value)
			response = conn.getresponse()
			print "Response =", response
			ret_val = response.read()
		except:
			return ret_val
		return ret_val
	def check_next(self, nextnode):
		try:
			value = "/is_alive"
			#params = urllib.urlencode({'@number': 12524, '@type': 'issue', '@action': 'show'})			
			#headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
			conn = httplib.HTTPConnection(nextnode, node_port)
			conn.request("GET", value)
			response = conn.getresponse()
			print "Response =", response
		except:
			return False
		return True		



	def check_node(self, ID):
		try:
			
			value = "/is_alive"
			#params = urllib.urlencode({'@number': 12524, '@type': 'issue', '@action': 'show'})			
			#headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
			conn = httplib.HTTPConnection(NodeList[ID], node_port)
			conn.request("GET", value)
			response = conn.getresponse()
			print "Response =", response
		except:
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

	for x in range (0,len(NodeList)):
		if socket.gethostname() == CorrectNodes[x]:
			if x == len(NodeList) - 1:
				NextNode = NodeList[0]
				Node_ID = x
			else:
				NextNode = NodeList[x+1]
				Node_ID = x


	try:
		logging.warning('Server setup!')
		httpd = BackEndHTTPServer(("",httpserver_port), BackEndHTTPHandler)
		Node_Score =  abs(hash(socket.gethostname()))
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