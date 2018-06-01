import threading
from util import safe_print

class Transaction:
	def __init__(self, debitNode, creditNode, cost):
		self.cost = cost
		self.creditNode = creditNode
		self.debitNode = debitNode
	def getCost(self):
		return self.cost
	def getCreditNode(self):
		return self.creditNode
	def getDebitNode(self):
		return self.debitNode
class TransactionManager:
	def __init__(self):
		self.balance = 100
		self.blockchain = []
		self.lock = threading.Lock()
		self.pendingTransactions = []
		self.pendingTransactionsCost = 0
	def addBlock(self, block):
		self.lock.acquire()
		self.deleteTransactions(block)
		self.blockchain.append(block)
		self.lock.release()
	def addPendingTransaction(self, transaction):
		self.lock.acquire()
		if len(self.pendingTransactions) >= 10:
			raise Error('There are too many transactions.')
		if self.pendingTransactionsCost + transaction.getCost() <= self.balance:
			self.pendingTransactions.append(transaction)
			self.pendingTransactionsCost += transaction.getCost()
		else:
			raise Error('You do not have enough money to pay this.')
		self.lock.release()
	def deleteTransactions(self, block):
		pass
	def getTransactionsForBlock(self):
		return self.pendingTransactions
	def getBalance(self):
		return self.balance
	def getBlockchain(self):
		return self.blockchain
	def getQueue(self):
		return self.pendingTransactions