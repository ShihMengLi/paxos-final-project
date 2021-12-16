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
server_ps = {} # id -> process
id = 1

parser = argparse.ArgumentParser()
parser.add_argument("--id", type=int, help="replica ID")
parser.add_argument("--recover", action='store_true', help="recover mode")
args = parser.parse_args()

print("starting servers %d..." % args.id)
p = subprocess.Popen(["python3", "paxos_acceptors.py", str(args.id), str(N_SERVERS), ""])
if args.id == 0:
    time.sleep(2)
if args.recover:
    p = subprocess.Popen(["python3", "paxos_proposers.py", str(args.id), str(N_SERVERS), "-vr"])
else:
    p = subprocess.Popen(["python3", "paxos_proposers.py", str(args.id), str(N_SERVERS), "-v"])
time.sleep(5)