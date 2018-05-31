import socket, threading
from message_templates import create_accept_msg, create_prepare_msg, create_ack_msg
from math import ceil

class PaxosManager:
	def __init__(self, globalConfig, serverI):
		self.globalConfig = globalConfig
		self.acceptNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.acceptVal = None
		self.ballotNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.depth = 0
		self.electionInProg = False
		self.isLeader = False
		self.acks = [] # should contain items of: { 'acceptNum': <int>, 'acceptVal': <list> }
		self.pid = globalConfig[serverI]['id']
		self.p1lock = threading.Lock() # phase 1 lock

	def broadcast(self, msg):
		clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		for broadcastAddr in self.globalConfig:
			clientSocket.sendto(msg, (broadcastAddr['ip_addr'], broadcastAddr['port']))
	def init_election(self):
		self.p1lock.acquire()
		self.acks = []
		self.acceptVal = [] # change this to some kind of get items from queue function
		self.ballotNum = { 'num': self.ballotNum['num'] + 1, 'pid': self.pid ,'depth': self.depth}
		self.electionInProg = True
		self.isLeader = False
		self.broadcast(create_prepare_msg(self.pid, self.ballotNum))
		self.p1lock.release()
	def process_recv_msg(self, msg):
		print str(msg)
		if msg['header']['type'] == 'accept':
			self.process_accept_msg(msg)
		elif msg['header']['type'] == 'prepare':
			self.process_prepare_msg(msg)
		elif msg['header']['type'] == 'ack':
			self.process_ack_msg(msg)
		else:
			raise Error('Incorrect msg format' + str(msg))
	def process_accept_msg(self, msg):
		pass
	def process_ack_msg(self, msg):
		self.p1lock.acquire()
		if self.isLeader or not self.electionInProg:
			return # if you already won the election or there is no election in progress, then ignore this ack msg
		if msg['header']['ballotNum'] == self.ballotNum:
			self.acks.append(msg['body'])
		if len(self.acks) >= (len(self.globalConfig) / 2): # majority acks
			self.isLeader = True
			val = self.__get_accept_val_from_acks()
			self.broadcast(create_accept_msg(self.pid, self.ballotNum, val))
		self.p1lock.release()

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
		maxAcceptVal = self.acceptVal
		maxAcceptNum = self.acceptNum
		for ack in self.acks:
			if ack['acceptVal'] is not None:
				pass
				# if ack['acceptNum']['depth'] >= maxAcceptNum['depth'] and ack['acceptNum']['num'] > ack
		return maxAcceptVal