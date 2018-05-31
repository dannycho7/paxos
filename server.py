import io, json, random, socket, sys, threading
from paxos_manager import PaxosManager
from time import sleep

if len(sys.argv) < 3:
	raise Exception('Wrong arguments. Correct Usage: python server.py <config_file_path> <server_index> <optional_dump_path>')
else:
	file = io.open(sys.argv[1])
	config_json = json.load(file)
	server_i = int(sys.argv[2]) % len(config_json)
	local_config = config_json[server_i]

val = server_i
paxos_m = PaxosManager(config_json, server_i)

def initPaxosThread():
	# initiate leader election within (0, 10] seconds of receiving a moneyTransfer
	while True:
		sleep(random.uniform(2,7))
		# if blockchain not empty
		paxos_m.init_election()

def listenRequests():
	serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	serverSocket.bind((local_config['ip_addr'], int(local_config['port'])))
	print "Server {0} listening on port {1}".format(str(local_config['id']), str(local_config['port']))

	while True:
		data, addr = serverSocket.recvfrom(1024)
		msg = json.loads(data)
		# print "Received message from server {0}.".format(msg['header']['pid'])
		paxos_m.process_recv_msg(msg)

t = threading.Thread(target=listenRequests)
t.daemon = True
t.start()

t = threading.Thread(target=initPaxosThread)
t.daemon = True
t.start()

while True:
	cmd = raw_input("")
	if "moneyTransfer" in cmd:
		pass
	elif cmd == "printBlockchain":
		pass
	elif cmd == "printBalance":
		pass
	elif cmd == "printQueue":
		pass
	else:
		print "Invalid command"
