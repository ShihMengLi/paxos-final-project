#/usr/bin/env python3

import random
import time
import signal
import sys
import json
import socket
import threading
from typing import List, Any, Dict, Set
from socketserver import TCPServer, BaseRequestHandler, ThreadingMixIn

PORT_ACCEPTOR = 10200            # group port range start, port = PORT + server_id
PORT_PROPOSER = 10210
PORT_REDIRECT = 10220
PORT_FETCH = 10230
PORT_HEARTBEAT = 10240  # group heartbeat port range start

class AcceptorHandler(BaseRequestHandler):
    '''
    Override the handle method to handle each request
    '''
    def handle (self):
        self.server.log("acquiring...")
        with self.server.handler_lock:
            data = self.request.recv(1024).strip().decode('utf-8')
            self.server.log('Received message {}'.format(data), verbose=True)
            try:
                reply_msg = self.server.process_msg(data)
                
                self.request.sendall(reply_msg.encode('utf-8'))
            except ValueError:
                self.server.log('Could not parse the data as String: {}'.format(data))
            finally:
                # close the connection because everything is async
                self.request.close()
            self.server.log("AcceptorHandler.handle done")

class AcceptServer(TCPServer):
    handler_lock = threading.Lock()
    # constructor
    def __init__ (self, sid: int, total: int):
        print("Acceptor SID = ", sid)
        self.maxProposalNum = 0
        self.corrProposalVal = str(-1)
        self.acceptorId = sid
        # self.statusDict = ["Wait", "Promise", "Accepted", "ACK"]
        # self.status = self.statusDict[0]
        addr = ('localhost', PORT_ACCEPTOR + sid)
        TCPServer.__init__(self, addr, AcceptorHandler)

    # receive a message from proposer
    def process_msg(self, data):
        reply_msg = "Empty Accept Msg"
        infos = data.split('_')
        if infos[0] == 'prepare':
            tmpProposalNum = int(infos[1])
            if tmpProposalNum >= self.maxProposalNum:
                reply_msg = 'promise_' + str(self.maxProposalNum) + '_' + self.corrProposalVal + '_' + str(self.acceptorId)
                self.maxProposalNum = tmpProposalNum
            else:
                reply_msg = 'promise_abort'
        elif infos[0] == 'accept':
            tmpAcceptNum = int(infos[1])
            tmpProposalVal = infos[2]
            if tmpAcceptNum >= self.maxProposalNum:
                self.maxProposalNum = tmpAcceptNum
                if 'lock' not in infos[2]:
                    self.corrProposalVal = tmpProposalVal
                reply_msg = 'accepted_' + infos[1] + '_' + infos[2]
            else:
                reply_msg = 'accepted_abort'
        self.log('Acceptor #{} Sending "{}" to Proposer #{}'.format(
                      self.acceptorId, reply_msg, infos[-1]))
        return reply_msg

    def get_next_instance_idx(self) -> int:
        if len(self.progress.keys()) == 0:
            return 0
        else:
            return max(self.progress.keys()) + 1

    def log (self, msg, verbose=False):
        return
        # if (not VERBOSE) and verbose:
        #     return
        # print('[A{}] {}'.format(self.acceptorId, msg))

# start server
def start (sid, total):
    server = AcceptServer(sid, total)
    # handle signal
    def sig_handler (signum, frame):
        server.server_close()
        exit(0)

    # register signal handler
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # serve until explicit shutdown
    ip, port = server.server_address
    server.log('Listening on {}:{} ...'.format(ip, port))
    server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage:\npython paxos_acceptor.py [server_id] [total_nodes] [-v]')
        exit(0)
    if len(sys.argv) == 4 and sys.argv[3] == "-v":
        VERBOSE = True
    else:
        VERBOSE = False

    start(int(sys.argv[1]), int(sys.argv[2]))
