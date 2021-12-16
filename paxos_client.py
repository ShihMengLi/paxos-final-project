#!/usr/bin/env python3

'''
A persistent client capable of sending multiple messages, resending messages on
timeout or failure, and remembering the stable paxos leader.
'''

import json
import signal
import sys
import socket
import time
from typing import Tuple, List
from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler
OKGREEN = '\033[92m'
RED = '\033[93m'
ENDC = '\033[0m'

MAX_RETRIES = 10

PORT_ACCEPTOR = 10200            # group port range start, port = PORT + server_id
PORT_PROPOSER = 10210
PORT_REDIRECT = 10220
PORT_FETCH = 10230
PORT_HEARTBEAT = 10240  # group heartbeat port range start

class LockClient(object):
    def __init__(self, client_id: int, total_servers: int):
        self.total_servers = total_servers
        self.client_id = client_id
        self.leader_id = 1
        self.retries = 0
        self.api_latencies = [] # type: List[float]
        self.command_id = 0

    def server_addr_port(self, proposer_id):
        return ('localhost', PORT_PROPOSER + proposer_id)

    def server_fetch_port(self, proposer_id):
        return ('localhost', PORT_FETCH + proposer_id)

    # def rotate_leader(self):
    #     self.leader_id = ((self.leader_id + 1) % self.total_servers) + 1

    def incr_and_get_retry(self):
        if self.retries > MAX_RETRIES:
            return None
        self.retries += 1
        return self.retries
    
    def send_msg(self, command: str, proposer_id: int, notIncreaseLamportTime=False):
        self.log('Client {} sending "{}" to Proposer {}'.format(self.client_id, command, proposer_id))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', PORT_PROPOSER + proposer_id)            
        try:
            sock.connect(self.server_addr_port(proposer_id))
            sock.sendall(command.encode('utf-8'))
            if notIncreaseLamportTime is False:
                self.command_id += 1
            rtMessage = sock.recv(1024).decode('utf-8')
            print(OKGREEN + "Client %d" % self.client_id + " Receive Message: '{}'".format(rtMessage) +\
                "from Proposer {}".format(proposer_id) + ENDC)
        except (ConnectionRefusedError, ConnectionResetError) as e:
            time.sleep(1)
            self.command_id -= 1
            return self.send_msg(command, (proposer_id + 1) % self.total_servers)
        except socket.timeout as e:
            print(RED + "Client %d" % self.client_id + \
                " Cannot Receive Message from Proposer {} before timeout".format(proposer_id) + ENDC)
            print(RED + "Client %d" % self.client_id + \
                " Send Request to Other Proposer(Proposer{})".format(proposer_id + 1) + ENDC)
            self.command_id -= 1
            return self.send_msg(command, (proposer_id + 1) % self.total_servers)
        finally:
            sock.close()
    
    def fetch(self, lock_id, proposer_id):    
        self.log('Client {} fetching "{}" from Proposer {}'.format(self.client_id, lock_id, proposer_id))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_msg = 'fetch_' + str(self.client_id) + '_' + str(lock_id)
        try:
            sock.connect(self.server_fetch_port(proposer_id))
            sock.sendall(send_msg.encode('utf-8'))
            recv_msg = sock.recv(1024).decode('utf-8')
            if "NoResult" in recv_msg:
                return "Still Processing..."
            else:
                return recv_msg
        except (ConnectionRefusedError, ConnectionResetError) as e:
            time.sleep(1)
            self.log('Proposer {} is down, try another'.format(proposer_id))
            return self.fetch(lock_id, (proposer_id + 1) % self.total_servers)
        finally:
            sock.close()
            
    def lock(self, lock_n: int, proposer_id: int, notIncreaseLamportTime=False):
        OKGREEN = '\033[92m'
        ENDC = '\033[0m'
        print(OKGREEN+"Client %d wants to lock %d by the %dth command via Proposer %d." % (self.client_id, lock_n, self.command_id, proposer_id) + ENDC)
        self.log("Client %d wants to lock %d by the %dth command via Proposer %d." % (self.client_id, lock_n, self.command_id, proposer_id))
        send_msg = 'client_' + str(self.client_id) + '_' + str(self.command_id) + '_lock-' + str(lock_n)
        self.send_msg(send_msg, proposer_id, notIncreaseLamportTime)
        
        return self.command_id - 1

    def unlock(self, lock_n: int, proposer_id: int):
        self.log("Client %d wants to unlock %d by the %dth command." % (self.client_id, lock_n, self.command_id))
        send_msg = 'client_' + str(self.client_id) + '_' + str(self.command_id) + '_unlock-' + str(lock_n)
        self.send_msg(send_msg, proposer_id)
        
        return self.command_id - 1

    def log (self, msg):
        return
        # print('[C{}] {}'.format(self.client_id, msg))
