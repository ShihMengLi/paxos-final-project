#!/usr/bin/env python3

'''
Usage: python test_run.py

Here we test normal lock service.
Test scenario:
 - One client
 - No node failures
 - Message loss rate of 5%
'''


import subprocess
import time
from paxos_client import LockClient
import threading

OKGREEN = '\033[92m'
YELLOW = '\033[93m'
ENDC = '\033[0m'

class LockServerThread(threading.Thread):
    def __init__(self, cli: LockClient):
        threading.Thread.__init__(self)
        self.cli = cli

    def run(self):
        self.cli.serve_forever()

N_SERVERS = 5
server_ps = {} # id -> process
server_ac = {} # id -> process

print("starting servers...")
for i in range(N_SERVERS):
    id_ = str(i)
    p = subprocess.Popen(["python3", "paxos_acceptors.py", id_, str(N_SERVERS), ""])
    server_ps[id_] = p

for i in range(N_SERVERS):
    id_ = str(i)
    p = subprocess.Popen(["python3", "paxos_proposers.py", id_, str(N_SERVERS), "-v"])
    server_ac[id_] = p
time.sleep(5)

# c0 lock 5 on p0 and c1 lock 10 on p0, then c0 unlock 5 on p0
print("init client and processing client lock")
cli0 = LockClient(0, N_SERVERS)
cli1 = LockClient(1, N_SERVERS)
cli2 = LockClient(2, N_SERVERS)
cli3 = LockClient(3, N_SERVERS)
cli4 = LockClient(4, N_SERVERS)


cli1.lock(5, 0)
cli2.lock(5, 0)
cli2.unlock(5, 0)

print(OKGREEN + "Client %d Fetch Lock Status %d from Proposer %d: " % (1, 5, 0), cli1.fetch(5, 0), ENDC)
print(OKGREEN + "Client %d Fetch Lock Status %d from Proposer %d: " % (1, 5, 0), cli1.fetch(5, 0), ENDC)
print(OKGREEN + "Client %d Fetch Lock Status %d from Proposer %d: " % (2, 5, 0), cli2.fetch(5, 0), ENDC)

print("stopping servers...")
for i in range(N_SERVERS):
    id_ = str(i)
    server_ps[id_].terminate()
print("stopping acceptors...")
for i in range(N_SERVERS):
    id_ = str(i)
    server_ac[id_].terminate()    
time.sleep(2)
