import io, json, random, socket, sys, threading
from paxos_manager import PaxosManager
from time import sleep
from transaction_manager import TransactionManager
from util import safe_print, ConnectGraph

if len(sys.argv) < 3:
	raise Exception('Wrong arguments. Correct Usage: python server.py <config_file_path> <server_index> <optional_dump_path>')
else:
	file = io.open(sys.argv[1])
	configJson = json.load(file)
	serverI = int(sys.argv[2]) % len(configJson)
	localConfig = configJson[serverI]

val = serverI
connectGraph = ConnectGraph(configJson)
transactionManager = TransactionManager(localConfig['id'])
paxosManager = PaxosManager(configJson, serverI, transactionManager, connectGraph)

currentSaveThreadNum = 0
saveThreadLock = threading.Lock() # used to prevent data race between currentSaveThread and adding a transaction that can create a saveThread

def initPaxosSaveThread(threadNum):
	# initiate leader election within (2, 10] seconds of receiving a moneyTransfer
	backoff = 10 # linearly increase this for increasing retry timeout
	backoffMax = 60 # maximum backoff of 60 seconds
	while True:
		timeout = random.uniform(backoff/4, backoff)
		safe_print('Starting save attempt in {0} seconds....'.format(timeout))
		sleep(timeout)
		saveThreadLock.acquire()
		if len(transactionManager.getQueue()) == 0 or currentSaveThreadNum != threadNum:
			saveThreadLock.release()
			break
		paxosManager.attempt_save() # won't save empty blocks
		backoff = min(backoffMax, 1.5 * backoff)
		saveThreadLock.release()

def refreshPaxosSaveThread():
	global currentSaveThreadNum
	currentSaveThreadNum += 1
	t = threading.Thread(target=initPaxosSaveThread, args=(currentSaveThreadNum,))
	t.daemon = True
	t.start()

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSocket.bind((localConfig['ip_addr'], int(localConfig['port'])))
print "Server {0} listening on port {1}".format(str(localConfig['id']), str(localConfig['port']))

if len(sys.argv) == 4:
	# load paxosManager from last saved
	paxosManager.initializeFromJSON(json.load(open(sys.argv[3])))
	if len(transactionManager.getQueue()) > 0: # if there was a transaction before the crash, start the save attempt thread
		refreshPaxosSaveThread()

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
		moneyTransferStr, creditNodeStr, costStr = cmd.split(',')
		transaction = { 'debitNode': localConfig['id'], 'creditNode': int(creditNodeStr), 'cost': int(costStr) }
		if transaction['creditNode'] < 0 or transaction['creditNode'] >= len(configJson):
			raise Exception('Invalid creditNode')
		else:
			saveThreadLock.acquire()
			transactionManager.addPendingTransaction(transaction)
			if len(transactionManager.getQueue()) == 1:
				refreshPaxosSaveThread()
			saveThreadLock.release()
	elif 'networkDown' in cmd:
		cmdName, pidStr = cmd.split(',')
		pid = int(pidStr)
		if pid == localConfig['id']:
			raise Error('You cannot take down a network communication between yourself')
		else:
			connectGraph.network_down(pid)
	elif 'networkUp' in cmd:
		cmdName, pidStr = cmd.split(',')
		pid = int(pidStr)
		connectGraph.network_up(pid)
	elif cmd == 'printBlockchain':
		blockchain = transactionManager.getBlockchain()
		safe_print(list(map(lambda block: str(block), blockchain)))
	elif cmd == 'printBalance':
		safe_print(transactionManager.getBalance())
	elif cmd == 'printQueue':
		safe_print(transactionManager.getQueue())
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