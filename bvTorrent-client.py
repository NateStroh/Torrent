#! /usr/bin/python3
from socket import *
import threading
import hashlib
import sys
import time
import random
import os

MAX_DOWNLOAD_THREADS = 4
MAX_SENDING_THREADS = 10

CLINPORT = 1234
TRAKIP= "10.152.1.131"
TRAKPORT = 42424

TRACKERCONN = socket(AF_INET, SOCK_STREAM)

chunkDesc = []
fileName = ""

## --- Variables that are cross-thread. We need to lock them in order to keep them in sync --- ##
chunkMaskLock = threading.Lock()
chunkMask = []

# {(TheirIP, TheirPort): [TheirChunkMask]}
downloadFromLock = threading.Lock()
downloadFrom = {}

# The number of threads currently running
downloadThreadsLock = threading.Lock()
currentDownloadThreads = 0

# Chunks that have yet to be claimed by a thread
unclaimedChunksLock = threading.Lock()
unclaimedChunks = []

def getLine(conn):
	msg = b''
	while True:
		ch = conn.recv(1)
		if ch == b'\n' or len(ch) == 0:
			break 
		msg += ch
	return msg.decode()

def getFullMsg(conn, msgLength):
	msg = b''
	while len(msg) < msgLength:
		retVal = conn.recv(msgLength - len(msg))
		msg += retVal
		if len(retVal) == 0:
			break
	return msg

# Gets the chunk(int) requested, returns true if it succesfully got the chunk and returns false if it did not
def getChunk(conn, chunkToGet):
	global fileName
	# SEND a single integer in form of string terminated by '\n' 
	s = str(chunkToGet)
	conn.send((s +'\n').encode())
	
	#Recieve chunk
	msg = getFullMsg(conn, int(chunkDesc[chunkToGet][0]))

	#run checksum to make sure data is right
	digest = hashlib.sha224(msg).hexdigest()
	if(digest != chunkDesc[chunkToGet][1]):
		print("Corrupted chunk: checksum is not equal", flush=True)
		unclaimedChunksLock.acquire()
		unclaimedChunks.append(chunkToGet)
		unclaimedChunksLock.release()
		return False
	
	chunkSize = int(chunkDesc[0][0])
	#write to file
	
	# Create file if it does not exist yet
	if (not os.path.exists(fileName)):
		with open(fileName, "x") as filecreator:
			pass
			
	with open(fileName, "r+b") as writer: 
		print("Seeking to: " + str(chunkToGet*chunkSize))
		writer.seek(chunkToGet*chunkSize)
		writer.write(msg)
	
	#update chunkMask
	chunkMaskLock.acquire()
	chunkMask[chunkToGet] = "1"
	
	print("Written chunk " + str(chunkToGet), flush=True)
	updateMask()
	chunkMaskLock.release()
	
	conn.close()
	downloadThreadsLock.acquire()
	global currentDownloadThreads
	currentDownloadThreads -= 1
	downloadThreadsLock.release()
	
	return True

def getClientWithChunk(chunkIndex):
	
	global CLINIP
	downloadFromLock.acquire()
	try:
		for addr, clientMask in downloadFrom.items():
			if(clientMask[chunkIndex] == "1"):	
				# Establish a connection with the client that has the chunk
				print("type: " + str(type(addr)))
				connIP = addr[0]
				connPort = addr[1]
				
				hostname = gethostname()
				ourIP = gethostbyname(hostname)
				
				if (connIP == ourIP and connPort == CLINPORT):
					continue
				
				print("Attempting connection to: " + str(connIP)+":"+str(connPort))
				
				clientCon = socket(AF_INET, SOCK_STREAM)
				clientCon.connect((connIP, int(connPort)))
				print("WE HAVE CONNECTED")
				downloadFromLock.release()
				return clientCon
		downloadFromLock.release()
		
	except Exception as e:
		#Update our snapshot of clients
		downloadFromLock.release()
		getClientList()
		#Call this again
		return getClientWithChunk(chunkIndex)	
	
def updateMask():
	#Send 12 byte control message to tell tracker we are sending out chunk list  
	TRACKERCONN.send("UPDATE_MASK\n".encode())
	
	# [Update Mask Protocol - Step 1 of 1]
	#  -SEND (numChunks) chars (1's and 0's) terminated by a "\n"
	chunkString = ''.join(chunkMask)
	TRACKERCONN.send((chunkString+'\n').encode())
	
	print("Mask updated", flush=True)
	
# TODO
def disconnect():
	#Send 12 byte control message to tracker to disconnect\
	TRACKERCONN.send("DISCONNECT!\n".encode())
	
	print("Disconnected from tracker", flush=True)

def getClientList():
	downloadFromLock.acquire()
	global downloadFrom
	#Send 12 byte control message to tell tracker we want a an updated client list 
	TRACKERCONN.send("CLIENT_LIST\n".encode())
	
	# [Client List Request Protocol - Step 1 of 2]
	#  - RECIEVE the number of clients in ASCII form terminated by "\n"
	x = getLine(TRACKERCONN)
	numClients = int(x)
	
	# [Client List Request Protocol - Step 2 of 2]
	#  - RECIEVE newline delimited descriptors of each client of the form:
	#	 clientListMsg = "client1IP:client1Port,client1ChunkMask\n" +
	#					 "client2IP:client2Port,client2ChunkMask\n" +
	#					 "client3IP:client3Port,client3ChunkMask\n" +
	#					 ...
	#					 "clientNIP:clientNPort,clientNChunkMask\n"
	# Store in downloadFrom { (clientIP,clientPort) : [chunkMask 
	for i in range(numClients):
		
		# Split Client Information
		clientInfo = getLine(TRACKERCONN).split(',')
		connInfo = clientInfo[0].split(':')
		clientMask = clientInfo[1]
		clientIP = connInfo[0]
		clientPort = connInfo[1]
		
		print(clientIP, clientPort, clientMask)
		
		# Cast clientMask to a list
		clientMask = list(clientMask)
		
		clientTup = (clientIP, clientPort)
		
		downloadFrom[clientTup] = clientMask
		
	downloadFromLock.release()

# Used for initiating a thread for a connection
def getChunkFromClient():
	global chunkMask
	global unclaimedChunks
	
	print("Chunky boi time", flush=True)
	
	unclaimedChunksLock.acquire()
	if (len(unclaimedChunks) > 0):
		chunkIndex = random.choice(unclaimedChunks)
		unclaimedChunks.remove(chunkIndex)
		unclaimedChunksLock.release()
		
		clientConn = getClientWithChunk(chunkIndex)
		
		hasChunk = getChunk(clientConn, chunkIndex)
		while (not hasChunk):
			hasChunk = getChunk(clientConn, chunkIndex)

	
# The base thread for downloads		
def beginDownloads():
	print("before", flush=True)
	getClientList()
	print("after", flush=True)
	while True:
		downloadThreadsLock.acquire()
		global currentDownloadThreads
		if (currentDownloadThreads < MAX_DOWNLOAD_THREADS):
			currentDownloadThreads += 1
			threading.Thread(target=getChunkFromClient, daemon=True).start()
		downloadThreadsLock.release()
				
# The base thread for sending the file
def beginSending():
	listener = socket(AF_INET, SOCK_STREAM)
	listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
	listener.bind(('', CLINPORT))
	listener.listen(MAX_SENDING_THREADS) 
	
	running = True
	while running:
		try:
			threading.Thread(target=handleClient, args=(listener.accept(),), daemon=True).start()
		except KeyboardInterrupt:
			print('\n[Shutting down]')
			running = False

			
# When a client connects to us to download, what do we do?
def handleClient(clientInfo):
	clientConn, clientAddr = clientInfo
	clientIP = clientAddr[0]
	print("Received connection from %s:%d" %(clientIP, clientAddr[1]))
	
	# Get the next line from the client, cast as an int
	chunkWanted = int(getLine(clientConn))
	sendChunk(clientConn, chunkWanted)

def sendChunk(conn, chunkIndex):
	global fileName
	with open(fileName, "rb") as chunkFile:
		# The first chunk should be the maximum size of a chunk
		#   We need to know this in order to find where we read from in file
		generalChunksize = int(chunkDesc[0][0])
		chunksize = int(chunkDesc[chunkIndex][0])
		
		chunkFile.seek(chunkIndex*generalChunksize)
		chunk = chunkFile.read(chunksize)
		conn.send(chunk)
	conn.close()
	
def main():
	global fileName
	global unclaimedChunks
	#Connect to Tracker 
	TRACKERCONN.connect((TRAKIP, TRAKPORT))
		
	# [New Connection Protocol - Step 1 of 5]
	#  - RECIEVE the filename terminated by "\n"
	fileName = getLine(TRACKERCONN)

	# [New Connection Protocol - Step 2 of 5]
	#  - RECIEVE the maximum chunk size in ASCII form terminated by "\n"
	CHUNKSIZE = int(getLine(TRACKERCONN))

	# [New Connection Protocol - Step 3 of 5]
	#  - RECIEVE the number of chunks in the file in ASCII form terminated by "\n"
	numChunks = int(getLine(TRACKERCONN))
	print("file: {} size:{} numChunks:{}".format(fileName,CHUNKSIZE,numChunks))

	# [New Connection Protocol - Step 4 of 5]
	#  - RECIEVE a newline delimited msg of chunk descriptors of the form:
	#	msg = "chunk1Size,chunk1digest\n" +
	#		  "chunk2Size,chunk2digest\n" +
	#		  "chunk3Size,chunk3digest\n" +
	#		  ....
	#		  "chunkNSize,chunkNdigest\n"

	for i in range(numChunks):
		chunkInfo = getLine(TRACKERCONN).split(',')
		chunkDesc.append((int(chunkInfo[0]), chunkInfo[1]))

	# [New Connection Protocol - Step 5 of 5]
	#   - SEND the port that the client will listen for connections on along
	#	 with the chunk mask that are delimited by a comma and newline 
	#	 terminated
	global chunkMask
	
	chunkMask = ['0'] * numChunks
	chunkString = '0' * numChunks
	message = str(CLINPORT) +',' +chunkString +'\n'
	TRACKERCONN.send(message.encode())

	unclaimedChunksLock.acquire()
	unclaimedChunks = [x for x in range(numChunks)]
	unclaimedChunksLock.release()
	
	# Start two threads - one that downloads, one that sends
	threading.Thread(target=beginDownloads, daemon=True).start()
	threading.Thread(target=beginSending, daemon=True).start()
	
main()
# Never disconnect - for good swarm health. We're good people like that.
