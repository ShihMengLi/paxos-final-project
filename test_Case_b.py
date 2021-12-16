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
    if i == 0 or i == 1:
        p = subprocess.Popen(["python3", "paxos_proposers.py", id_, str(N_SERVERS), ""])
    else:
        p = subprocess.Popen(["python3", "paxos_proposers.py", id_, str(N_SERVERS), ""])
    server_ac[id_] = p
time.sleep(10)

# c0 lock 5 on p0 and c1 lock 10 on p0, then c0 unlock 5 on p0
print("init client and processing client lock")
cli0 = LockClient(0, N_SERVERS)
cli1 = LockClient(1, N_SERVERS)
cli2 = LockClient(2, N_SERVERS)
cli3 = LockClient(3, N_SERVERS)
cli4 = LockClient(4, N_SERVERS)


cli0.lock(lock_n=5, proposer_id=0, notIncreaseLamportTime=True)
cli0.lock(lock_n=5, proposer_id=1, notIncreaseLamportTime=True)
time.sleep(2)
cli0.fetch(lock_id=5, proposer_id=0)
cli0.fetch(lock_id=5, proposer_id=1)
time.sleep(5)



# for i in range(N_SERVERS):
#     id_ = str(i)
#     server_ps[id_].terminate()
# print("Terminate All")
# commandId = cli.lock(1, 0)
# time.sleep(1)
# print(cli.fetch(commandId, 0))
# print("processing client unlock")
# assert cli.unlock(1) == {'status': 'ok'}
# assert cli.lock(1) == {'status': 'ok'}

# msg_counts = []
# for i in range(N_SERVERS):
#     cli.leader_id = i + 1
#     msg_counts.append(cli.get_msg_count()['msg_count'])

print("stopping servers...")
for i in range(N_SERVERS):
    id_ = str(i)
    server_ps[id_].terminate()
print("stopping acceptors...")
for i in range(N_SERVERS):
    id_ = str(i)
    server_ac[id_].terminate()    
time.sleep(2)
# print("msg_counts: {}".format(msg_counts))
# print("client API latencies: {}".format(cli.api_latencies))
