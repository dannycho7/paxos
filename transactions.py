import threading
from util import safe_print

class TransactionManager:
	def __init__(self, pid):
		self.balance = 100
		self.blockchain = []
		self.lock = threading.Lock()
		self.pendingTransactions = []
		self.pendingTransactionsCost = 0
		self.pid = pid
	def addBlock(self, block):
		self.lock.acquire()
		self.deleteTransactions(block)
		for transaction in block:
			if transaction['creditNode'] == self.pid:
				self.balance += transaction['cost']
			if transaction['debitNode'] == self.pid:
				self.balance -= transaction['cost']
		self.blockchain.append(block)
		self.lock.release()
	def addPendingTransaction(self, transaction):
		self.lock.acquire()
		if len(self.pendingTransactions) >= 10:
			raise Exception('There are too many transactions.')
		if self.pendingTransactionsCost + transaction['cost'] <= self.balance:
			self.pendingTransactions.append(transaction)
			self.pendingTransactionsCost += transaction['cost']
		else:
			raise Exception('You do not have enough money to pay this.')
		self.lock.release()
	def deleteTransaction(self, transaction):
		for i in range(0, len(self.pendingTransactions)):
			if transaction == self.pendingTransactions[i]:
				self.pendingTransactions = self.pendingTransactions[0:i] + self.pendingTransactions[i + 1:]
				self.pendingTransactionsCost -= transaction['cost']
				transFound = True
				break
	def deleteTransactions(self, block):
		for transaction in block:
			self.deleteTransaction(transaction)	
	def getTransactionsForBlock(self):
		return self.pendingTransactions
	def getBalance(self):
		return self.balance
	def getBlockchain(self):
		return self.blockchain
	def getQueue(self):
		return self.pendingTransactions