import threading, random, socket
from time import sleep

class DelayedSocket:
	def __init__(self):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	def sendto(self, msg, destTuple):
		sleep(random.uniform(1.8,2))
		self.sock.sendto(msg, destTuple)

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