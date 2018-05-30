import io, json, socket, sys, threading

if len(sys.argv) < 3:
	raise Exception('Wrong arguments. Correct Usage: python server.py <config_file_path> <server_index> <optional_dump_path>')
else:
	file = io.open(sys.argv[1])
	config_json = json.load(file)
	server_i = int(sys.argv[2]) % len(config_json)
	config = config_json[server_i]

def initPaxosThread():
	# initiate leader election within (0, 10] seconds of receiving a moneyTransfer
	seq_num = 0
	while True:
		sleep(random.uniform(0,10))
		# msg = "accept\n[{0},{1},{2}]".format(seq_num, config['id'], depth = 1)
		# clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		# clientSocket.sendto(msg, (send_to_config['ip_addr'], send_to_config['port']))

def listenRequests():
	serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	serverSocket.bind((config['ip_addr'], int(config['port'])))
	print "Server {0} listening on port {1}".format(str(config['id']), str(config['port']))

	while True:
		data, addr = serverSocket.recvfrom(1024)
		header, body = list(map(lambda x: json.loads(x), data.split("|")))
		print "Received message from server {0}.".format(header[1])

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
