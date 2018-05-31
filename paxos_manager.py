import socket, threading
from math import ceil
from message_templates import create_accept_msg, create_prepare_msg, create_ack_msg
from util import safe_print

class PaxosManager:
	def __init__(self, globalConfig, serverI):
		self.globalConfig = globalConfig
		self.acceptNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.acceptVal = []
		self.ballotNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.depth = 0
		self.electionInProg = False
		self.isLeader = False
		self.acks = [] # should contain items of: { 'acceptNum': <int>, 'acceptVal': <list> }
		self.pid = globalConfig[serverI]['id']
		self.lock = threading.Lock() # reduces complexity by only processing one message at a time/preventing a collision from starting an election and processing a msg at the same time.
	def broadcast(self, msg):
		clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		for broadcastAddr in self.globalConfig:
			clientSocket.sendto(msg, (broadcastAddr['ip_addr'], broadcastAddr['port']))
	def init_election(self):
		self.lock.acquire()
		self.acks = []
		self.acceptVal = [] # change this to some kind of get items from queue function
		self.ballotNum = { 'num': self.ballotNum['num'] + 1, 'pid': self.pid ,'depth': self.depth}
		self.electionInProg = True
		self.isLeader = False
		self.broadcast(create_prepare_msg(self.pid, self.ballotNum))
		self.lock.release()
	def process_recv_msg(self, msg):
		self.lock.acquire()
		safe_print("Received message: {0}".format(str(msg)))
		if msg['header']['type'] == 'accept':
			self.process_accept_msg(msg)
		elif msg['header']['type'] == 'prepare':
			self.process_prepare_msg(msg)
		elif msg['header']['type'] == 'ack':
			self.process_ack_msg(msg)
		else:
			raise Error('Incorrect msg format' + str(msg))
		self.lock.release()
	def process_accept_msg(self, msg):
		pass
	def process_ack_msg(self, msg):
		if self.isLeader or not self.electionInProg:
			return # if you already won the election or there is no election in progress, then ignore this ack msg
		if msg['header']['ballotNum'] == self.ballotNum:
			self.acks.append(msg['body'])
		if len(self.acks) > (len(self.globalConfig) / 2): # majority acks
			self.isLeader = True
			val = self.__get_accept_val_from_acks()
			self.broadcast(create_accept_msg(self.pid, self.ballotNum, val))
	def process_prepare_msg(self, msg):
		pid = msg['header']['pid']
		ballotNum = msg['header']['ballotNum']
		if ballotNum['depth'] < self.depth:
			return
		elif ballotNum['depth'] > self.depth:
			# maybe do something since it seems like you'd be out-of-date
			pass
		if ballotNum['num'] > self.ballotNum['num'] or (ballotNum['num'] == self.ballotNum['num'] and ballotNum['pid'] >= self.ballotNum['pid']):
			if ballotNum != self.ballotNum:
				self.electionInProg = False # cancel election since you've ack'd a ballotNum higher than yourself
			self.ballotNum = ballotNum
			ack_msg = create_ack_msg(self.pid, self.ballotNum, self.acceptNum, self.acceptVal)
			clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			clientSocket.sendto(ack_msg, (self.globalConfig[pid]['ip_addr'], self.globalConfig[pid]['port']))
	def __get_accept_val_from_acks(self):
		# You don't have to check for depth, because it is checked on the process_prepare_msg. A node would NOT send an ACK if the depth is different from the prepared ballotNum. acceptVal/Num should be refreshed on every depth update.
		maxAcceptVal = self.acceptVal
		maxAcceptNum = self.acceptNum
		for ack in self.acks:
			if ack['acceptVal'] is not None:
				if ack['acceptNum']['num'] > maxAcceptNum['num'] or (ack['acceptNum']['num'] == maxAcceptNum['num'] and ack['acceptNum']['pid'] > maxAcceptNum['pid']):
					maxAcceptNum = ack['acceptNum']
					maxAcceptVal = ack['acceptVal']
		return maxAcceptVal