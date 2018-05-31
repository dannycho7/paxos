import json

def create_msg(msg_type, pid, ballotNum, body = []):
	msg = {}
	msg['header'] = {}
	msg['header']['ballotNum'] = ballotNum
	msg['header']['pid'] = pid
	msg['header']['type'] = msg_type
	msg['body'] = body
	return json.dumps(msg)
def create_accept_msg(pid, ballotNum, body):
	return create_msg('accept', pid, ballotNum, body)
def create_ack_msg(pid, ballotNum, acceptNum, acceptVal):
	body = {}
	body['acceptNum'] = acceptNum
	body['acceptVal'] = acceptVal
	return create_msg('ack', pid, ballotNum, body)
def create_prepare_msg(pid, ballotNum):
	return create_msg('prepare', pid, ballotNum)