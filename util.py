#!/usr/bin/python2
import threading, random, socket
from time import sleep

class ConnectGraph:
	def __init__(self, globalConfig):
		self.globalConfig = globalConfig
		self.graph = {}
		for conf in self.globalConfig:
			self.graph[str(conf['id'])] = True
	def initializeFromJSON(self, props):
		self.graph = props['graph']
		self.globalConfig = props['globalConfig']
	def json(self):
		return { 'globalConfig': self.globalConfig, 'graph': self.graph }
	def getStatus(self, pid):
		return self.graph[str(pid)]
	def network_down(self, pid):
		self.graph[str(pid)] = False
	def network_up(self, pid):
		self.graph[str(pid)] = True
	def __str__(self):
		return str(self.graph)

class DelayedSocket:
	def __init__(self, connectGraph):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.connectGraph = connectGraph
	def delayed_send(self, msg, destTuple, pid):
		t = threading.Thread(target=self.sendto, args=(msg, destTuple, pid,))
		t.daemon = True
		t.start()
	def initializeFromJSON(self, props):
		self.connectGraph.initializeFromJSON(props['connectGraph'])
	def json(self):
		return { 'connectGraph': self.connectGraph.json() }
	def sendto(self, msg, destTuple, pid):
		sleep(random.uniform(1, 1.5))
		if self.connectGraph.getStatus(pid) == True:
			self.sock.sendto(msg, destTuple)
		else:
			safe_print('Unable to reach node {0}'.format(pid))
class SafePrint:
	def __init__(self):
		self.lock = threading.Lock()
	def safe_print(self, msg):
		self.lock.acquire()
		print msg
		self.lock.release()

safePrintObj = SafePrint()

def safe_print(msg):
	safePrintObj.safe_print(msg)