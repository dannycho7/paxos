import io, json, random, socket, sys, threading
from paxos_manager import PaxosManager
from time import sleep
from transactions import TransactionManager
from util import safe_print

if len(sys.argv) < 3:
	raise Exception('Wrong arguments. Correct Usage: python server.py <config_file_path> <server_index> <optional_dump_path>')
else:
	file = io.open(sys.argv[1])
	configJson = json.load(file)
	serverI = int(sys.argv[2]) % len(configJson)
	localConfig = configJson[serverI]

val = serverI
transactionManager = TransactionManager(localConfig['id'])
paxos_m = PaxosManager(configJson, serverI, transactionManager)

def initPaxosThread():
	# initiate leader election within (0, 10] seconds of receiving a moneyTransfer
	while True:
		sleep(random.uniform(2,7))
		# if blockchain not empty

def listenRequests():
	serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	serverSocket.bind((localConfig['ip_addr'], int(localConfig['port'])))
	print "Server {0} listening on port {1}".format(str(localConfig['id']), str(localConfig['port']))

	while True:
		data, addr = serverSocket.recvfrom(1024)
		msg = json.loads(data)
		# print "Received message from server {0}.".format(msg['header']['pid'])
		threading.Thread(target=paxos_m.process_recv_msg, args=(msg,)).start()

t = threading.Thread(target=listenRequests)
t.daemon = True
t.start()

t = threading.Thread(target=initPaxosThread)
t.daemon = True
t.start()

while True:
	cmd = raw_input("")
	if "moneyTransfer" in cmd:
		moneyTransferStr, creditNodeStr, costStr = cmd.split(',')
		transaction = { 'debitNode': localConfig['id'], 'creditNode': int(creditNodeStr), 'cost': int(costStr) }
		transactionManager.addPendingTransaction(transaction)
	elif cmd == "printBlockchain":
		blockchain = transactionManager.getBlockchain()
		safe_print(list(map(lambda block: str(block), blockchain)))
	elif cmd == "printBalance":
		safe_print(transactionManager.getBalance())
	elif cmd == "printQueue":
		safe_print(transactionManager.getQueue())
	elif cmd == "attemptSave":
		paxos_m.attempt_save()
	else:
		print "Invalid command"
