#!/usr/bin/python2
import io, json, socket, sys, threading
from paxos_manager import PaxosManager
from transaction_manager import TransactionManager
from util import safe_print, ConnectGraph

if len(sys.argv) < 3:
	raise Exception('Wrong arguments. Correct Usage: python server.py <config_file_path> <server_index> <optional_dump_path>')
else:
	file = io.open(sys.argv[1])
	configJson = json.load(file)
	serverI = int(sys.argv[2]) % len(configJson)
	localConfig = configJson[serverI]
	serverIdList = map(lambda x: x['id'], configJson)

val = serverI
connectGraph = ConnectGraph(configJson)
transactionManager = TransactionManager(localConfig['id'])
paxosManager = PaxosManager(configJson, serverI, transactionManager, connectGraph)

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSocket.bind((localConfig['local_ip_addr'], int(localConfig['port'])))
print "Server {0} listening on port {1}".format(str(localConfig['id']), str(localConfig['port']))

if len(sys.argv) == 4:
	# load paxosManager from last saved
	paxosManager.initializeFromJSON(json.load(open(sys.argv[3])))
	if len(transactionManager.getQueue()) > 0: # if there was a transaction before the crash, start the save attempt thread
		paxosManager.attempt_save_timeout_refresh()

def listenRequests():
	while True:
		data, addr = serverSocket.recvfrom(1024)
		msg = json.loads(data)
		threading.Thread(target=paxosManager.process_recv_msg, args=(msg,)).start()

t = threading.Thread(target=listenRequests)
t.daemon = True
t.start()

while True:
	cmd = raw_input("")
	if 'moneyTransfer' in cmd:
		moneyTransferStr, creditNode, costStr = cmd.split(',')
		transaction = { 'debitNode': localConfig['id'], 'creditNode': creditNode, 'cost': int(costStr) }
		if transaction['creditNode'] not in serverIdList:
			safe_print('Invalid creditNode')
		else:
			try:
				paxosManager.add_transaction(transaction)
			except Exception as e:
				safe_print(e)
	elif 'networkDown' in cmd:
		cmdName, pid = cmd.split(',')
		if pid == localConfig['id']:
			safe_print('You cannot take down a network communication between yourself')
		else:
			connectGraph.network_down(pid)
	elif 'networkUp' in cmd:
		cmdName, pid = cmd.split(',')
		if pid == localConfig['id']:
			safe_print('You cannot take down a network communication between yourself')
		else:
			connectGraph.network_up(pid)
	elif cmd == 'printBlockchain':
		blockchain = transactionManager.getBlockchain()
		blockchainStr = []
		for i in range(0, len(blockchain)):
			block = blockchain[i]
			blockStr = []
			for transaction in block:
				blockStr.append(transactionManager.getTransactionStr(transaction))
			blockchainStr.append("Block {0}: {1}".format(i, '; '.join(blockStr)))
		safe_print('\n'.join(blockchainStr))
	elif cmd == 'printBalance':
		safe_print(transactionManager.getBalance())
	elif cmd == 'printQueue':
		block = transactionManager.getQueue()
		blockStr = []
		for transaction in block:
			blockStr.append(transactionManager.getTransactionStr(transaction))
		if len(blockStr) == 0:
			blockStrRepr = "None"
		else:
			blockStrRepr = '; '.join(blockStr)
		safe_print("Queued Transactions: {0}".format(blockStrRepr))
	elif cmd == 'attemptSave':
		paxosManager.attempt_save()
	elif cmd == 'serverCrash':
		paxosManager.lock.acquire() # take the paxosManager lock and don't release it
		paxosManager.dumpDisk()
		safe_print('server crashing')
		sys.exit()
	elif cmd == 'printGraph':
		print connectGraph
	else:
		print 'Invalid command'