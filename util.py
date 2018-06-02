import threading, random, socket
from time import sleep

class DelayedSocket:
	def __init__(self, globalConfig):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.globalConfig = globalConfig
		self.connectGraph = {}
		for conf in self.globalConfig:
			self.connectGraph[conf['id']] = True
	def delayed_send(self, msg, destTuple, pid):
		t = threading.Thread(target=self.sendto, args=(msg, destTuple, pid,))
		t.daemon = True
		t.start()
	def sendto(self, msg, destTuple, pid):
		sleep(random.uniform(1.8,2))
		if self.connectGraph[pid] == True:
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