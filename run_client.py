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
import argparse

N_SERVERS = 5

parser = argparse.ArgumentParser()
parser.add_argument("--id", type=int, help="client ID")
args = parser.parse_args()

class LockServerThread(threading.Thread):
    def __init__(self, cli: LockClient):
        threading.Thread.__init__(self)
        self.cli = cli

    def run(self):
        self.cli.serve_forever()

print("starting client %d..." % args.id)
cli = LockClient(args.id, N_SERVERS)
while(1):
    command = input("Enter Command [lock/unlock/fetch]:").strip()
    if "fetch" == command.lower():
        lock_id = input("Fetch Lock [0~10]:").strip()
        replica_id = input("From Replica [0~4]:").strip()
        lock_id = int(lock_id)
        replica_id = int(replica_id)
        print(cli.fetch(lock_id, replica_id))
    elif "unlock" == command.lower():
        lock_id = input("UnLock [0~10]:").strip()
        replica_id = input("From Replica [0~4]:").strip()
        lock_id = int(lock_id)
        replica_id = int(replica_id)
        print("UnLock %d via Proposer %d" % (lock_id, replica_id), cli.unlock(lock_id, replica_id))
    elif "lock" == command.lower():
        lock_id = input("Lock [0~10]:").strip()
        replica_id = input("From Replica [0~4]:").strip()
        lock_id = int(lock_id)
        replica_id = int(replica_id)
        print("Process")
        print("Lock %d via Proposer %d" % (lock_id, replica_id), cli.lock(lock_id, replica_id))
    else:
        print("Invalid input, please enter again.")
# print("starting servers...")
# for i in range(N_SERVERS):
#     id_ = str(i)
#     server_ps[id_] = p

# for i in range(N_SERVERS):
#     id_ = str(i)
#     server_ps[id_] = p
# time.sleep(5)

# # c0 lock 5 on p0 and c1 lock 10 on p0, then c0 unlock 5 on p0
# print("init client and processing client lock")
# cli1 = LockClient(1, N_SERVERS)
# cli2 = LockClient(2, N_SERVERS)
# cli3 = LockClient(3, N_SERVERS)
# cli4 = LockClient(4, N_SERVERS)
# cli5 = LockClient(5, N_SERVERS)


# cli1.lock(5, 0)
# cli2.lock(10, 0)
# cli1.lock(5, 0)

# time.sleep(5)

# print("Fetch Lock %d from Proposer %d" % (5, 0), cli1.fetch(5, 0))
# print("Fetch Lock %d from Proposer %d" % (5, 1), cli1.fetch(5, 1))
# print("Fetch Lock %d from Proposer %d" % (10, 0), cli1.fetch(10, 0))
# print("Fetch Lock %d from Proposer %d" % (10, 1), cli1.fetch(10, 1))

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

# print("stopping servers...")
# for i in range(N_SERVERS):
#     id_ = str(i + 1)
#     server_ps[id_].terminate()
# time.sleep(2)
# print("msg_counts: {}".format(msg_counts))
# print("client API latencies: {}".format(cli.api_latencies))
