import json, random, threading
from math import ceil
from message_templates import create_accept_msg, create_ack_msg, create_block_update_req_msg, create_block_update_res_msg, create_decision_msg, create_prepare_msg
from time import sleep
from util import DelayedSocket, safe_print

class PaxosManager:
	def __init__(self, globalConfig, serverI, transactionManager, connectGraph):
		self.globalConfig = globalConfig
		self.currentSaveThreadNum = 0
		self.depth = len(transactionManager.getBlockchain())
		self.saveThreadLock = threading.Lock() # data race between incrementing self.currentSaveThreadNum and the check for it in the thread. (not critical but can be annoying).
		self.lock = threading.Lock() # reduces complexity by only processing one message at a time/preventing a collision from starting an election and processing a msg at the same time.
		self.pid = globalConfig[serverI]['id']
		self.serverI = serverI
		self.sock = DelayedSocket(connectGraph) # share one socket on PaxosManager since every method is thread-safe
		self.transactionManager = transactionManager
		self.hard_reset_activity()	
	def reset_activity(self):
		# simply an assignment method that should be called when wanting to reset the variables that are used to determine progress in paxos. used when starting a new election
		self.acceptCount = 0
		self.acks = [] # should contain items of: { 'acceptNum': <int>, 'acceptVal': <list> }
		self.electionInProg = False
		self.isLeader = False
	def hard_reset_activity(self):
		# also sets acceptVal and acceptNum to init values. used when deciding on a value in process_accept_msg or when creating new paxos instance
		self.reset_activity()
		self.ballotNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.acceptNum = { 'num': 0, 'depth': 0, 'pid': 0 }
		self.acceptVal = None	
	def add_transaction(self, transaction):
		self.lock.acquire()
		self.transactionManager.addPendingTransaction(transaction)
		if len(self.transactionManager.getQueue()) == 1:
			self.attempt_save_timeout_refresh()
		self.lock.release()
	def attempt_save(self):
		self.lock.acquire()
		if len(self.transactionManager.getQueue()) > 0:
			self.init_election()
		self.dumpDisk()
		self.attempt_save_timeout_refresh()
		self.lock.release()
	def attempt_save_timeout_refresh(self):
		self.kill_current_save_timeout()
		if len(self.transactionManager.getQueue()) == 0:
			return
		t = threading.Thread(target=self.attempt_save_timeout_thread, args=(self.currentSaveThreadNum,))
		t.daemon = True
		t.start()
	def attempt_save_timeout_thread(self, threadNum):
		# initiate leader election within (12, 20] seconds of receiving a moneyTransfer
		timeout = random.uniform(12, 20)
		sleep(timeout)
		self.saveThreadLock.acquire()
		if self.currentSaveThreadNum != threadNum:
			self.saveThreadLock.release()
			return
		self.saveThreadLock.release()
		safe_print('Starting save attempt after {0} seconds....'.format(timeout))
		self.attempt_save() # won't save empty blocks
	def kill_current_save_timeout(self):
		self.saveThreadLock.acquire()
		self.currentSaveThreadNum += 1 # kill the current save thread loop
		self.saveThreadLock.release()
	def broadcast(self, msg):
		# does not broadcast to self
		for config in self.globalConfig[0:self.serverI] + self.globalConfig[self.serverI + 1:]:
			self.sock.delayed_send(msg, (config['ip_addr'], config['port']), config['id'])
	def init_election(self):
		self.reset_activity()
		self.ballotNum = { 'num': self.ballotNum['num'] + 1, 'pid': self.pid ,'depth': self.depth}
		self.electionInProg = True
		prepare_msg = create_prepare_msg(self.pid, self.ballotNum)
		self.process_prepare_msg(json.loads(prepare_msg))
		self.broadcast(prepare_msg)
	def dumpDisk(self):
		with open("./server-{0}.dump.json".format(self.pid), 'w+') as f:
			json.dump(self.json(), f)
	def initializeFromJSON(self, props):
		self.globalConfig = props['globalConfig']
		self.depth = props['depth']
		self.pid = props['pid']
		self.serverI = props['serverI']
		self.transactionManager.initializeFromJSON(props['transactionManager'])
		self.acceptCount = props['acceptCount']
		self.acks = props['acks']
		self.ballotNum = props['ballotNum']
		self.electionInProg = props['electionInProg']
		self.isLeader = props['isLeader']
		self.acceptNum = props['acceptNum']
		self.acceptVal = props['acceptVal']
		self.sock.initializeFromJSON(props['sock'])
	def json(self):
		props = {}
		props['globalConfig'] = self.globalConfig
		props['depth'] = self.depth
		props['pid'] = self.pid
		props['serverI'] = self.serverI
		props['transactionManager'] = self.transactionManager.json()
		props['acceptCount'] = self.acceptCount
		props['acks'] = self.acks
		props['ballotNum'] = self.ballotNum
		props['electionInProg'] = self.electionInProg
		props['isLeader'] = self.isLeader
		props['acceptNum'] = self.acceptNum
		props['acceptVal'] = self.acceptVal
		props['sock'] = self.sock.json()
		return props
	def process_recv_msg(self, msg):
		self.lock.acquire()
		safe_print("Received message: {0}".format(str(msg)))
		msg_pid = msg['header']['pid']
		msg_type = msg['header']['type']
		msg_ballotNum = msg['header']['ballotNum']
		if msg_type == 'blockUpdateReq':
			self.process_block_update_req_msg(msg)
		elif msg_type == 'blockUpdateRes':
			self.process_block_update_res_msg(msg)
		elif msg_ballotNum['depth'] < self.depth:
			# this means that an outdated node sent you a message
			# forge a blockUpdateReq message from that node so that you can process it locally and send back a blockUpdateRes
			sendToConfig = self.globalConfig[self.__get_server_index_from_pid(self.pid)] # send to yourself
			block_update_req_msg = create_block_update_req_msg(msg_pid, msg_ballotNum) # forge message as if it was created by the node
			self.sock.delayed_send(block_update_req_msg, (sendToConfig['ip_addr'], sendToConfig['port']), msg_pid)
		elif msg_ballotNum['depth'] > self.depth:
			# send blockchain request to the node you received a msg from since you'd be out-of-date
			sendToConfig = self.globalConfig[self.__get_server_index_from_pid(msg_pid)]
			block_update_req_msg = create_block_update_req_msg(self.pid, self.ballotNum)
			self.sock.delayed_send(block_update_req_msg, (sendToConfig['ip_addr'], sendToConfig['port']), msg_pid)
		elif msg_type == 'accept':
			self.process_accept_msg(msg)
		elif msg_type == 'ack':
			self.process_ack_msg(msg)
		elif msg_type == 'decision':
			self.process_decision_msg(msg)
		elif msg_type == 'prepare':
			self.process_prepare_msg(msg)
		else:
			raise Exception('Incorrect msg format' + str(msg))
		self.dumpDisk()
		self.lock.release()
	def process_accept_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		msg_pid = msg['header']['pid']
		sendToConfig = self.globalConfig[self.__get_server_index_from_pid(msg_pid)]
		if self.pid == ballotNum['pid'] and self.ballotNum == ballotNum: # check leadership by seeing if the proposed ballot was from this node
			self.acceptNum = ballotNum
			self.acceptVal = msg['body']
			self.acceptCount += 1
			if self.acceptCount > (len(self.globalConfig) / 2): # receive accept from majority
				decision_msg = create_decision_msg(self.pid, self.ballotNum, self.acceptVal)
				self.process_decision_msg(json.loads(decision_msg))
				self.attempt_save_timeout_refresh()
				self.broadcast(decision_msg)
		elif ballotNum['num'] > self.ballotNum['num'] or (ballotNum['num'] == self.ballotNum['num'] and ballotNum['pid'] >= self.ballotNum['pid']):
			self.acceptNum = ballotNum
			self.acceptVal = msg['body']
			accept_msg = create_accept_msg(self.pid, self.ballotNum, self.acceptVal)
			self.sock.delayed_send(accept_msg, (sendToConfig['ip_addr'], sendToConfig['port']), msg_pid)
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
	def process_block_update_req_msg(self, msg):
		msg_pid = msg['header']['pid']
		sendToConfig = self.globalConfig[self.__get_server_index_from_pid(msg_pid)]
		block_update_res_msg = create_block_update_res_msg(self.pid, self.ballotNum, self.transactionManager.getBlockchain())
		self.sock.delayed_send(block_update_res_msg, (sendToConfig['ip_addr'], sendToConfig['port']), msg_pid)
	def process_block_update_res_msg(self, msg):
		msg_blockchain = msg['body']
		for i in range(self.depth, len(msg_blockchain)):
			self.transactionManager.addBlock(msg_blockchain[i])
		self.depth = len(self.transactionManager.getBlockchain())
	def process_decision_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		self.transactionManager.addBlock(msg['body'])
		self.depth += 1
		self.hard_reset_activity()
	def process_prepare_msg(self, msg):
		ballotNum = msg['header']['ballotNum']
		msg_pid = msg['header']['pid']
		sendToConfig = self.globalConfig[self.__get_server_index_from_pid(msg_pid)]
		if ballotNum['num'] > self.ballotNum['num'] or (ballotNum['num'] == self.ballotNum['num'] and ballotNum['pid'] >= self.ballotNum['pid']):
			if ballotNum != self.ballotNum:
				self.electionInProg = False # cancel election since you've ack'd a ballotNum higher than yourself
			self.ballotNum = ballotNum
			ack_msg = create_ack_msg(self.pid, self.ballotNum, self.acceptNum, self.acceptVal)
			self.sock.delayed_send(ack_msg, (sendToConfig['ip_addr'], sendToConfig['port']), msg_pid)
			self.attempt_save_timeout_refresh()
	def __get_server_index_from_pid(self, msg_pid):
		for i in range(0, len(self.globalConfig)):
			if msg_pid == self.globalConfig[i]['id']:
				return i
		raise Exception('Invalid msg_pid')
	def __get_accept_val_from_acks(self):
		# You don't have to check for depth, because it is checked on the process_prepare_msg. A node would NOT send an ACK if the depth is different from the prepared ballotNum. acceptVal/Num should be refreshed on every depth update.
		maxAcceptVal = self.transactionManager.getTransactionsForBlock()
		maxAcceptNum = { 'num': 0, 'depth': 0, 'pid': 0 } # lowest possible acceptNum. used for condition so that we use our val if no other accepted values
		for ack in self.acks:
			if ack['acceptVal'] is not None:
				if ack['acceptNum']['num'] > maxAcceptNum['num'] or (ack['acceptNum']['num'] == maxAcceptNum['num'] and ack['acceptNum']['pid'] > maxAcceptNum['pid']):
					maxAcceptNum = ack['acceptNum']
					maxAcceptVal = ack['acceptVal']
		return maxAcceptVal