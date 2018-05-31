import threading

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