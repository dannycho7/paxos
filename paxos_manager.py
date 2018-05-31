import json, socket, threading
from math import ceil
from message_templates import create_accept_msg, create_ack_msg, create_decision_msg, create_prepare_msg
from util import safe_print

class PaxosManager:
	def __init__(self, globalConfig, serverI):
		self.globalConfig = globalConfig
		self.depth = 0
		self.lock = threading.Lock() # reduces complexity by only processing one message at a time/preventing a collision from starting an election and processing a msg at the same time.
		self.pid = globalConfig[serverI]['id']
		self.serverI = serverI
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # share one socket on PaxosManager since every method is thread-safe
		self.reset_activity()
	def reset_activity(self):
		# simply an assignment method that should be called when wanting to reset the variables that are used to determine progress in paxos. used when starting a new election or when deciding on a value in process_accept_msg
		self.acceptCount = 0
		self.acceptNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.acceptVal = None
		self.acks = [] # should contain items of: { 'acceptNum': <int>, 'acceptVal': <list> }
		self.ballotNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.electionInProg = False
		self.isLeader = False
	def broadcast(self, msg):
		# does not broadcast to self
		for broadcastAddr in self.globalConfig[0:self.serverI] + self.globalConfig[self.serverI + 1:]:
			self.sock.sendto(msg, (broadcastAddr['ip_addr'], broadcastAddr['port']))
	def init_election(self):
		self.lock.acquire()
		self.reset_activity()
		self.acceptVal = [] # change this to some kind of get items from queue function
		self.ballotNum = { 'num': self.ballotNum['num'] + 1, 'pid': self.pid ,'depth': self.depth}
		self.electionInProg = True
		prepare_msg = create_prepare_msg(self.pid, self.ballotNum)
		self.process_prepare_msg(json.loads(prepare_msg))
		self.broadcast(prepare_msg)
		self.lock.release()
	def process_recv_msg(self, msg):
		self.lock.acquire()
		safe_print("Received message: {0}".format(str(msg)))
		if msg['header']['type'] == 'accept':
			self.process_accept_msg(msg)
		elif msg['header']['type'] == 'ack':
			self.process_ack_msg(msg)
		elif msg['header']['type'] == 'decision':
			self.process_decision_msg(msg)
		elif msg['header']['type'] == 'prepare':
			self.process_prepare_msg(msg)
		else:
			raise Error('Incorrect msg format' + str(msg))
		self.lock.release()
	def process_accept_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		msg_pid = msg['header']['pid']
		if ballotNum['depth'] < self.depth:
			return
		elif ballotNum['depth'] > self.depth:
			# maybe do something since it seems like you'd be out-of-date
			pass
		if self.pid == ballotNum['pid'] and self.ballotNum == ballotNum: # check leadership by seeing if the proposed ballot was from this node
			self.acceptNum = ballotNum
			self.acceptVal = msg['body']
			self.acceptCount += 1
			if self.acceptCount > (len(self.globalConfig) / 2): # receive accept from majority
				decision_msg = create_decision_msg(self.pid, self.ballotNum, self.acceptVal)
				self.process_decision_msg(json.loads(decision_msg))
				self.broadcast(decision_msg)
		elif ballotNum['num'] > self.ballotNum['num'] or (ballotNum['num'] == self.ballotNum['num'] and ballotNum['pid'] >= self.ballotNum['pid']):
			self.acceptNum = ballotNum
			self.acceptVal = msg['body']
			accept_msg = create_accept_msg(self.pid, self.ballotNum, self.acceptVal)
			self.sock.sendto(accept_msg, (self.globalConfig[msg_pid]['ip_addr'], self.globalConfig[msg_pid]['port']))
	def process_ack_msg(self, msg):
		if self.isLeader or not self.electionInProg:
			return # if you already won the election or there is no election in progress, then ignore this ack msg
		if msg['header']['ballotNum'] == self.ballotNum:
			self.acks.append(msg['body'])
		if len(self.acks) > (len(self.globalConfig) / 2): # majority acks
			self.isLeader = True
			self.electionInProg = False
			val = self.__get_accept_val_from_acks()
			accept_msg = create_accept_msg(self.pid, self.ballotNum, val)
			self.process_accept_msg(json.loads(accept_msg))
			self.broadcast(accept_msg)
	def process_decision_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		if self.depth != ballotNum['depth']:
			return
		# add msg['body'] to blockchain
		self.depth += 1
		self.reset_activity()
	def process_prepare_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		msg_pid = msg['header']['pid']
		if ballotNum['depth'] < self.depth:
			return
		elif ballotNum['depth'] > self.depth:
			# maybe do something since it seems like you'd be out-of-date
			return
		if ballotNum['num'] > self.ballotNum['num'] or (ballotNum['num'] == self.ballotNum['num'] and ballotNum['pid'] >= self.ballotNum['pid']):
			if ballotNum != self.ballotNum:
				self.electionInProg = False # cancel election since you've ack'd a ballotNum higher than yourself
			self.ballotNum = ballotNum
			ack_msg = create_ack_msg(self.pid, self.ballotNum, self.acceptNum, self.acceptVal)
			self.sock.sendto(ack_msg, (self.globalConfig[msg_pid]['ip_addr'], self.globalConfig[msg_pid]['port']))
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