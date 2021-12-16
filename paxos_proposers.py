#/usr/bin/env python3

import random
import time
import signal
import sys
import json
import socket
import threading
import numpy as np
from typing import List, Any, Dict, Set
from socketserver import TCPServer, BaseRequestHandler, ThreadingMixIn

OKGREEN = '\033[92m'
YELLOW = '\033[93m'
ENDC = '\033[0m'

PORT_ACCEPTOR = 10200            # group port range start, port = PORT + server_id
PORT_PROPOSER = 10210
PORT_REDIRECT = 10220
PORT_FETCH = 10230
PORT_HEARTBEAT = 10240  # group heartbeat port range start
WAITING_TIMEOUT = 3    # time to wait before resending potentially lost msgs

# enums for state machine command sequence
S_UNSURE = 'undefined'
S_NO_OP = 'no-op'
S_ELECT_LEADER_PREFIX = 'S_ELECT_LEADER_'
def S_ELECT_LEADER(leader_id):
    return S_ELECT_LEADER_PREFIX + str(leader_id)

# enums for paxos message types
T_PREP = 'prepare'
T_PREP_R = 'prepare-reply'
T_ACC = 'accept'
T_ACC_R = 'accept-reply'
T_LEARN = 'learn'

# phases
P_PREP = 0
P_ACC = 1
P_LEARN = 2

# waiting status, used in resend logic to handle network outage
W_OK = 0
W_WAITING_PREPARE_REPLIES = 1
W_WAITING_ACCEPT_REPLIES = 2
# TODO: waiting on learn?

def INIT_PROGRESS():
    return {
        'phase': P_PREP,
        'base_n': 0, # proposal number base
        'highest_proposed_v': '',
        'highest_proposed_n': -1, # highest proposal number I've promised
        'prepare': {}, # stores prepare responses
        'accepted_reply': {} # stores accept responses
    }

class HeartBeatRecvHandler(BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip().decode('utf-8')
        try:
            self.server.recv_heartbeat(data)
        except ValueError:
            self.server.lock_server.log('[HeartbeatRecv] Could not parse the data as JSON: {}'.format(data))
        finally:
            # close the connection because everything is async
            self.request.close()

class HeartBeatRecvServer(TCPServer):
    allow_reuse_address = True
    last_heartbeat = None
    def __init__ (self, sid: int, lock_server):
        addr = ('localhost', PORT_HEARTBEAT + sid)
        self.heartrecvaddr = addr
        self.sid = sid
        self.lock_server = lock_server
        self.leaderHeartBeat = (-1, None)
        TCPServer.__init__(self, addr, HeartBeatRecvHandler)
    
    def recv_heartbeat(self, data):
        infos = data.split('_')
        leader_id = int(infos[1])
        self.lock_server.pastSlotId = int(infos[2])
        # if len(infos) > 2:
        #     for singleInfo in infos[3:]:
        #         if singleInfo[0] == 'a':
        #             self.lock_server.failed_acceptors.add(int(singleInfo[1:]))
        #         if singleInfo[0] == 'p':
        #             self.lock_server.failed_proposers.add(int(singleInfo[1:]))
        # self.lock_server.log("Receive heart beat from Leader %s" % leader_id)
        if self.lock_server.leader == -1 and leader_id >= 0:
            self.lock_server.log(OKGREEN + "Change leader to {}".format(leader_id) + ENDC)
            self.lock_server.leader = leader_id
            self.leaderHeartBeat = (self.lock_server.leader, time.time())
            if self.lock_server.recover is True:
                self.request_decide()            
        elif self.lock_server.leader == leader_id:
            self.leaderHeartBeat = (self.lock_server.leader, time.time())
        elif self.lock_server.leader != leader_id:
            self.lock_server.log(OKGREEN + "Change leader to {}".format(leader_id) + ENDC)
            self.lock_server.leader = leader_id
            self.leaderHeartBeat = (self.lock_server.leader, time.time())
            if self.lock_server.recover is True:
                self.request_decide()
        # self.lock_server.log('Proposer #{} update heartbeat "{}" from Leader #{}'.format(
        #               self.lock_server.server_id, self.leaderHeartBeat, leader_id))
    
    def request_decide(self):
        send_msg = "requestDecide_%d" % (self.lock_server.server_id)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = ('localhost', PORT_PROPOSER + self.lock_server.leader)
        sock.settimeout(0.5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        try:
            sock.connect(addr)
            sock.sendall(send_msg.encode('utf-8'))
        except ConnectionRefusedError:
            self.lock_server.log("Recover {} recover fail, please kill the process".format(self.lock_server.server_id))
            pass
        except socket.timeout as e:
            pass
        finally:
            sock.close()

class HeartBeatRecvThread(threading.Thread):
    def __init__(self, sid: int, lock_server):
        threading.Thread.__init__(self)
        self.server = HeartBeatRecvServer(sid, lock_server)
        
    def run(self):
        self.server.lock_server.log("Proposor %d start to run heartbeat receiver at PORT %s" % (self.server.sid, self.server.heartrecvaddr))
        self.server.serve_forever()

class HeartBeatCheckerThread(threading.Thread):
    def __init__(self, heartbeat_server, lock_server):
        threading.Thread.__init__(self)
        self.heartbeat_server = heartbeat_server
        self.lock_server = lock_server
        self.stopped = False

    def run(self):
        while not self.stopped:
            if self.lock_server.is_leader() is False:
                # self.lock_server.log('heartbeat information {}'.format(self.heartbeat_server.leaderHeartBeat))
                if self.heartbeat_server.leaderHeartBeat[0] >= 0 and self.heartbeat_server.leaderHeartBeat[1] is not None:
                    wait_time = (self.lock_server.server_id) * 1 + 1
                    if float(self.heartbeat_server.leaderHeartBeat[1]) + wait_time < time.time():
                        self.lock_server.log('heartbeat check failed, trigger election')
                        print("TimeOut = ", time.time() - self.heartbeat_server.leaderHeartBeat[1] + wait_time)
                        self.lock_server.failed_proposers.add(self.lock_server.leader)
                        self.lock_server.leader = -1
                        self.lock_server.electLeader(increaseProposalNum=True)
                        while self.lock_server.leader == -1:
                            time.sleep(self.lock_server.abortWaitTime)
                            self.lock_server.electLeader(increaseProposalNum=True)
                        # break
                    else:
                        self.abortWaitTime = 1 + self.lock_server.server_id * 0.2
            else:
                self.abortWaitTime = 1 + self.lock_server.server_id * 0.2
            time.sleep(1.0)

class HeartBeatSenderThread(threading.Thread):
    def __init__(self, lock_server):
        threading.Thread.__init__(self)
        self.lock_server = lock_server
        self.stopped = False

    def run(self):
        time.sleep(0.5)
        self.lock_server.log("Proposor %d start to serves heartbeat service" % (self.lock_server.server_id))
        while not self.stopped:
            self.lock_server.send_heartbeat()
            if self.lock_server.recover is True and self.lock_server.leader >= 0:
                data = self.send_requestRecover()
                if data.split('_')[-1].lower() == "end":
                    self.lock_server.log("Recover {}, recover success!".format(self.lock_server.server_id))
                    self.lock_server.recover = False
                else:
                    Cliend_Id, CmdId, SlotId, Cmd = [x for x in data.split('_')[1:]]
                    Cliend_Id = int(Cliend_Id)
                    CmdId = int(CmdId)
                    SlotId = int(SlotId)
                    self.lock_server.pastCommands[(Cliend_Id, CmdId)] = (SlotId, Cmd)
            time.sleep(0.5)

    def send_requestRecover(self):
        send_msg = 'requestRecover_{}'.format(self.lock_server.server_id)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = ('localhost', PORT_PROPOSER + self.lock_server.leader)
        self.lock_server.log('Recover {} sending "{}" to Leader {}'.format(
          self.lock_server.server_id, send_msg, self.lock_server.leader))
        try:
            sock.connect(addr)
            sock.sendall(send_msg.encode('utf-8'))
            try:
                rtMessage = sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.lock_server.log("Recover {} recover fail, please kill the process".format(self.lock_server.server_id))
                raise e
            except socket.timeout as e:
                print("Recover node connect to leader Node {} is Time Out!, please kill the process".format(self.lock_server.server_id))
                raise e
        except ConnectionRefusedError as e:
            self.lock_server.log("Recover {} recover fail, please kill the process".format(self.lock_server.server_id))
            raise e
        finally:
            sock.close()
            return rtMessage

class FetchRecvHandler(BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024).strip().decode('utf-8')
        infos = data.split('_')
        clientId = int(infos[1])
        lockId = int(infos[2])
        rtMessage = self.server.fetchData(clientId, lockId)
        try:
            self.server.lock_server.log('Sendding result "{}" to client {}'.format(rtMessage, clientId))
            self.request.sendall(rtMessage.encode('utf-8'))
        except ValueError:
            self.server.lock_server.log('[FetchRecv] Could not understand the fetch')
        finally:
            # close the connection because everything is async
            self.request.close()

class FetchRecvServer(TCPServer):
    allow_reuse_address = True
    last_heartbeat = None
    def __init__ (self, sid: int, lock_server):
        addr = ('localhost', PORT_FETCH + sid)
        self.sid = sid
        self.lock_server = lock_server
        TCPServer.__init__(self, addr, FetchRecvHandler)
    
    def fetchData(self, clientId, lockId):
        self.lock_server.log('Client {} fetching data "{},lock No.{}" from Proposor {}'.format(clientId, clientId, lockId, self.lock_server.server_id))
        pastCmdList = []
        printStr = '{'
        for k, v in self.lock_server.pastCommands.items():
            printStr += '(Client ID: %d,  Lamport TimeStamp: %d, Request: %s)' % (k[0], k[1], v[1])
        printStr += '}' 
        self.lock_server.log('Client %d check status of lock %d' % (clientId, lockId))
        self.lock_server.log(OKGREEN + 'Current Processed Sequence Request: {}'.format(printStr) + ENDC)
        for k, v in self.lock_server.pastCommands.items():
            pastCmdList.append((k[0], k[1], v[0], v[1])) # (Cliend Id, CmdId, SlotId, Cmd)
        pastCmdList.sort(key = lambda x: (x[2], x[0], x[1])) # (SlotId, Cliend Id, CmdId)
        self.lock_server.log('Print Past Command = {}'.format(pastCmdList))
        self.lock_server.locks = [None for _ in  self.lock_server.locks]
        for cmd in pastCmdList:
            self.execute(cmd[0], cmd[1], cmd[3])
        if self.lock_server.locks[lockId] == None:
            rtMessage = "Lock %d is free." % lockId
        else:
            rtMessage = "Lock %d is owned by %d." % (lockId, self.lock_server.locks[lockId])
        # elif self.lock_server.locks[lockId] != clientId:
        #     rtMessage = "Lock %d is owned by %d." % (lockId, self.lock_server.locks[lockId])
        # elif self.lock_server.locks[lockId] == clientId:
        #     rtMessage = "Lock %d belongs to you." % lockId
        self.lock_server.locks = [None for _ in  self.lock_server.locks]
        # self.lock_server.log('Data "{},lock No.{}" Result: "{}"'.format(clientId, lockId, rtMessage))
        
        return rtMessage
        
    def execute(self, clientId: int, cmdId: int, command: str):
        if "l" == command[0]:
            lockNode = int(command.split('-')[-1])
            if self.lock_server.locks[lockNode] == None:
                print("Lock %d is locked by %d" % (lockNode, clientId))
                self.lock_server.locks[lockNode] = clientId
            elif self.lock_server.locks[lockNode] == clientId:
                print("Lock %d is already locked by %d" % (lockNode, clientId))
                pass
            elif self.lock_server.locks[lockNode] != clientId:
                print("Lock %d cannot be locked by %d" % (lockNode, clientId))
                pass
        elif "u" == command[0]:
            unlockNode = int(command.split('-')[-1])
            if self.lock_server.locks[unlockNode] == None:
                print("Lock %d is already freed" % unlockNode)
                pass
            elif self.lock_server.locks[unlockNode] == clientId:
                self.lock_server.locks[unlockNode] = None
                print("Lock %d is unlocked by %d" % (unlockNode, clientId))
            elif self.lock_server.locks[unlockNode] != clientId:
                print("Lock %d cannot be unlocked by %d" % (unlockNode, clientId))
                pass

class FetchRecvThread(threading.Thread):
    def __init__(self, sid: int, lock_server):
        threading.Thread.__init__(self)
        self.server = FetchRecvServer(sid, lock_server)
        
    def run(self):
        self.server.lock_server.log("Proposor %d start to serves fetch service" % (self.server.sid))
        self.server.serve_forever()




class RedirectSenderThread(threading.Thread):
    def __init__(self, lock_server):
        threading.Thread.__init__(self)
        self.lock_server = lock_server
        self.stopped = False
        

    def run(self):
        time.sleep(1.0)
        self.lock_server.log("Proposor %d start to serves redirect service" % (self.lock_server.server_id))
        while not self.stopped:
            self.lock_server.redirect()
            time.sleep(1.0)

class LockHandler(BaseRequestHandler):
    '''
    Override the handle method to handle each request
    '''
    def handle(self):
        self.server.log("acquiring...")
        # with self.server.handler_lock:
        data = self.request.recv(1024).strip().decode('utf-8')
        self.server.log('Received message {}'.format(data), verbose=True)
        try:
            # data = json.loads(data)
            infos = data.split('_')
            self.server.log('Infos = {}'.format(infos), verbose=True)
            if infos[0] == 'client':
                self.server.processClient(infos)
                reply_msg = \
                    'Request (Client ID: %d,  Lamport TimeStamp: %d, Request: %s) has been processed' % \
                        (int(infos[1]), int(infos[2]), infos[3])
                self.request.sendall(reply_msg.encode('utf-8'))
            elif infos[0] == 'promise':
                self.server.processPromise(infos)
            elif infos[0] == 'accepted':
                self.server.processAccepted(infos)
            elif infos[0] == 'decide':
                reply_msg = self.server.processDecide(infos)
                self.request.sendall(reply_msg.encode('utf-8'))
            elif infos[0] == 'leader':
                reply_msg = self.server.processLeader(infos)
                self.request.sendall(reply_msg.encode('utf-8'))
            elif infos[0] == 'requestDecide':
                self.server.generateRecoverCommands(int(infos[1]))
                self.server.sendDecideProposers.add(int(infos[1]))
            elif infos[0] == 'requestRecover':
                reply_msg = self.server.processRecoverCommands(int(infos[1]))
                self.request.settimeout(1)
                try:
                    self.request.sendall(reply_msg.encode('utf-8'))
                except ConnectionRefusedError:
                    print("Connection to recover Node {} is Refused!".format(int(infos[1])))
                except socket.timeout as e:
                    print("Connection to recover Node {} is Time Out!".format(int(infos[1])))
                finally:
                    pass
                self.request.settimeout(None)
                if reply_msg.split('_')[-1].lower() == 'end':
                    self.server.sendDecideProposers.discard(int(infos[1]))
            elif infos[0] == 'recover':
                self.server.failed_proposers.discard(int(infos[1]))
                self.server.failed_acceptors.discard(int(infos[1]))
            # else:
            #     self.server.receive_msg(self.request, data)
        except ValueError:
            self.server.log('Could not parse the data as String: {}'.format(data))
        finally:
            # close the connection because everything is async
            self.request.close()
        self.server.log("LockHandler.handle done")

class LockServer(TCPServer):
    # whether the server will allow the reuse of an address
    allow_reuse_address = True

    leader = -1 # initial leader
    is_electing = False # if this node is during the election period

    # state machine
    next_exe = 0 # the next executable command index
    sendDecideProposers = set()
    # an array of command sequence
    # the i-th element corresponds to the result of the i-th paxos instance
    # undecided commands are marked by S_UNSURE
    states = [] # type: List[str]
    pastCommands = {} # Key = (clientId, cmdId), Value = (SlotId, Command, Result)
    recoverCommands = {} # Key = RecoverId, Value = (Cliend Id, CmdId, SlotId, Cmd)
    futureCommands = {} # Key = (clientId, cmdId), Value = Command Message
    locks = [None for _ in range(11)] # locked by client n or None
    proposerNumber = None
    msg_count = 0
    slotId = 0
    pastSlotId = -1
    # stores per instance information
    # leader: prepare responses, accept responses, proposal number
    # follower: highest promised proposal number, highest accept
    progress = {} # type: Dict[int, Any]

    failed_proposers = set() # type: Set[int]
    failed_acceptors = set()
    
    handler_lock = threading.Lock()

    waiting = W_OK
    waiting_settime = 0
    wait_args = None

    # constructor
    def __init__ (self, sid: int, total: int, recover=False):
        self.server_id = sid
        self.total_nodes = total
        self.proposal_Number = total - sid
        self.abortWaitTime = 1 + self.server_id * 0.2
        self.recover = recover
        addr = ('localhost', PORT_PROPOSER + sid)
        TCPServer.__init__(self, addr, LockHandler)
        self.waitTime = 1 + sid * 0.2
        if recover is False:
            while(self.leader == -1 and self.server_id == 0):
                time.sleep(self.abortWaitTime)
                self.electLeader()
    def generateRecoverCommands(self, recoverId):
        if len(self.recoverCommands.get(recoverId, [])) == 0:
            self.log("Generate Recover Command for Recover Node %d" % recoverId)
            self.recoverCommands[recoverId] = []
            for xx in self.pastCommands.items(): # ((Cliend Id, CmdId), (SlotId, Cmd))
                print("Recover Command Generated = ", xx)
                self.recoverCommands[recoverId].append([str(xx[0][0]), str(xx[0][1]), str(xx[1][0]), str(xx[1][1])])
        
    # TODO: Add this back, send heartbeat to followers
    def send_heartbeat(self):
        if self.server_id == self.leader:
            msg = self.server_id
            send_msg = "heart_%d_%d" % (self.server_id, self.slotId)
            for i in range(self.total_nodes):
                if not i == self.server_id:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    addr = ('localhost', PORT_HEARTBEAT + i)
                    # self.log('Leader #{} sending "{}" to Proposor #{}'.format(
                    #   self.server_id, send_msg, i))
                    try:
                        sock.connect(addr)
                        sock.sendall(send_msg.encode('utf-8'))
                    except ConnectionRefusedError:
                        pass
                        # print("Connection to proposer {} is Refused!".format(i))
                        # if i not in self.failed_proposers:                        
                        #     self.failed_proposers.add(i)
                    finally:
                        sock.close()

    # TODO: Add this back, send heartbeat to followers
    def processRecoverCommands(self, recoverId):
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock.settimeout(0.5)
        # sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # addr = ('localhost', PORT_PROPOSER + recoverId)
        if len(self.recoverCommands[recoverId]) > 0:
            send_msg = "recoverCommand_%s" % '_'.join(self.recoverCommands[recoverId].pop())
        else:
            send_msg = "recoverCommand_end"
        # try:
        #     sock.connect(addr)
        #     sock.sendall(send_msg.encode('utf-8'))
        # except ConnectionRefusedError:
        #     print("Connection to recover Node {} is Refused!".format(recoverId))
        #     send_msg = 'fail'
        # except socket.timeout as e:
        #     print("Connection to recover Node {} is Time Out!".format(recoverId))
        #     send_msg = 'fail'
        # finally:
        #     sock.close()

        return send_msg

    # TODO: Add this back, send message to leaders
    def redirect(self):
        removeCmds = []
        for k in self.futureCommands.keys():
            if self.pastCommands.get(k) is not None:
                removeCmds.append(k)
        for rmk in removeCmds:
            self.futureCommands.pop(rmk)
        for k in self.futureCommands.keys():
            if self.pastCommands.get(k) is None:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                other_node = random.choice([xx for xx in range(self.total_nodes) if xx not in self.failed_proposers and xx != self.leader])
                if self.leader == self.server_id or self.leader == -1:
                    addr = ('localhost', PORT_PROPOSER + other_node)
                    self.log('Leader {} redirect "{}" to Proposer {}'.format(
                        self.server_id, self.futureCommands[k], other_node))                    
                else:
                    addr = ('localhost', PORT_PROPOSER + self.leader)
                    self.log('Proposer {} redirect "{}" to Leader {}'.format(
                        self.server_id, self.futureCommands[k], self.leader))
                try:
                    sock.connect(addr)
                    sock.sendall(self.futureCommands[k].encode('utf-8'))
                except ConnectionRefusedError:
                    pass
                    # print("Connection Refused!!!")
                except socket.timeout as e:
                    self.log("socket timeout {}\nRedirect from Proposer{}: to Leader: {}".format(e, self.server_id, self.leader))
                finally:
                    sock.close()
                    break

    # TODO: Add this back, send heartbeat to followers
    def recv_heartbeat(self, data):
        leader_id = int(data.split('_')[1])
        timestamp = float(data.split('_')[2])
        
        if self.leader == -1 and leader_id >= 0:
            self.leader = leader_id
            self.leaderHeartBeat = (self.leader, time.time())
        elif self.leader == leader_id:
            self.leaderHeartBeat = (self.leader, time.time())
        elif self.leader != leader_id:
            self.leader = leader_id
            self.leaderHeartBeat = (self.leader, time.time())
        # self.log('Proposer #{} Receiving "{}" from Leader #{}'.format(
        #               self.server_id, data, self.leader))
        return

    def log (self, msg, verbose=False):
        if (not VERBOSE) and verbose:
            return
        print('[S{}] {}'.format(self.server_id, msg))

    def is_leader (self):
        return self.leader == self.server_id

    def electLeader(self, increaseProposalNum=False):
        self.abortWaitTime *= 1.5
        if increaseProposalNum is True:
            self.proposal_Number += self.total_nodes
        send_msg = 'prepare_%d_%d' % (self.proposal_Number, self.server_id)
        tmpRecvs = []
        if self.leader != -1: return
        for i in range(self.total_nodes):
            if i in self.failed_acceptors:
                continue
            tmpRecvs.append(None)
            tmpRecvs[-1] = self.send_prepare(defaultPORT=PORT_ACCEPTOR, acceptorId=i, clientId=-1, cmdId=-1, payload=send_msg)
            if "abort" in tmpRecvs[-1]:
                return
        validRecvs = [(int(xx.split('_')[-1]), xx) for xx in tmpRecvs if "promise" in xx]
        if len(validRecvs) >= (self.total_nodes // 2) + 1:
            recvNoVal = {}
            for _, recv in validRecvs:
                recvNo = int(recv.split('_')[1])
                recvVal = int(recv.split('_')[2])
                if recvVal >= 0:
                    recvNoVal[recvNo] = recvVal
            if len(recvNoVal.keys()) > 0:
                self.log("Receive Accepted Value = {}".format(int(recvNoVal[sorted(recvNoVal.keys())[-1]])))
                self.log("FAILED_PROPOSERS = {}".format(self.failed_proposers))
            if len(recvNoVal.keys()) == 0:
                myVal = self.server_id
            elif int(recvNoVal[sorted(recvNoVal.keys())[-1]]) in self.failed_proposers:
                myVal = self.server_id
            else:
                myVal = recvNoVal[sorted(recvNoVal.keys())[-1]]
            send_msg = 'accept_' + str(self.proposal_Number) + '_' + str(myVal) + '_' + str(self.server_id)
            self.log("Sendding Accept Message = {}".format(send_msg))
            tmpRecvs = []
            validAccepted = 0
            for accId, _ in validRecvs:                    
                if i in self.failed_acceptors:
                    continue
                tmpRecvs.append(None)
                tmpRecvs[-1] = self.send_elect_accept(defaultPORT=PORT_ACCEPTOR, acceptorId=accId, payload=send_msg)
                if 'accepted' in tmpRecvs[-1]:
                    validAccepted += 1
                # if validAccepted >= self.total_nodes // 2 + 1:
                #     break
            if validAccepted >= self.total_nodes // 2 + 1:
                append_msg = ''
                if len(self.failed_proposers) > 0:
                    append_msg += '_' + ('_').join([str(x) for x in self.failed_proposers])
                send_msg = 'leader_' + str(myVal) + append_msg
                for i in range(self.total_nodes):
                    if i in self.failed_proposers or i == self.server_id:
                        continue
                    reply_msg = self.send_elect_decide(defaultPORT=PORT_PROPOSER, proposerId=i, payload=send_msg, toWhom='proposer')
                    if "leader" not in reply_msg or int(reply_msg.split('_')[1]) != myVal:
                        break
                self.leader = myVal
                self.slotId = self.pastSlotId + 1
                return
        self.proposal_Number = self.proposal_Number + self.total_nodes

    def processLeader(self, infos):
        leaderId = int(infos[1])
        if len(infos) > 2:
            for xx in infos[2:]:
                self.failed_proposers.add(int(xx))
        self.leader = leaderId
        self.log(OKGREEN + "Change leader to {}".format(self.leader) + ENDC)
        # self.log("Proposer%d's choose %d as leader" % (self.server_id, self.leader))
        
        reply_msg = 'leader_' + str(self.leader)
        
        return reply_msg

    # send prepare messages to all nodes
    def send_prepare(self, defaultPORT: int, acceptorId: int, clientId: int, cmdId: int, payload):
        if random.random() > 1.00:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, acceptorId) + "^" * 80)
            return "TimeOut"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', defaultPORT + acceptorId)
        try:
            sock.connect(addr)
            sock.sendall((payload).encode('utf-8'))
            self.log('Proposer #{} Sending prepare_{} to Acceptor #{}'.format(
                      self.server_id, payload.split('_')[1], acceptorId))
            try:
                rtMessage = sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.log(">>>>>>\nacceptorId: {}\npayload: {}\n<<<<<<<".format(acceptorId, payload))
                raise e
        except ConnectionRefusedError:
            self.log("Acceptor #{} is down, removing it".format(acceptorId))
            self.failed_acceptors.add(acceptorId)
            rtMessage = "NoAcceptor"
        except socket.timeout as e:
            self.log("socket timeout {}\nAcceptor_id: {}\nCommandInfo: ({}, {})".format(e, acceptorId, clientId, cmdId))
            
        finally:
            sock.close()
            return rtMessage
            
    # send decide messages to other proposers(replicas)
    def send_elect_decide(self, defaultPORT: int, proposerId: int, payload, toWhom):
        if random.random() > 1.00:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, proposerId) + "^" * 80)
            time.sleep(1)
            return "TimeOut"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', defaultPORT + proposerId)
        try:
            sock.connect(addr)
            sock.sendall((payload).encode('utf-8'))
            self.log('Proposer #{} Sending decide Leader #{} to {} #{}'.format(
                      self.server_id, self.server_id, toWhom, proposerId))
            try:
                return sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.log(">>>>>>\n{}}Id: {}\npayload: {}\n<<<<<<<".format(toWhom, proposerId, payload))
                raise e
        except ConnectionRefusedError:
            self.log("{} #{} is down, removing it".format(toWhom, proposerId))
            if toWhom.lower() == "proposer":
                self.failed_proposers.add(proposerId)
                return "NoProposer"
            elif toWhom.lower() == "acceptor":
                self.failed_acceptors.add(proposerId)
                return "Acceptor"
        except socket.timeout as e:
            self.log("socket timeout {}\n To {}_id #{} Elect Leader #{}".format(e, toWhom, proposerId, self.server_id))
            return "TimeOut"
        finally:
            sock.close()

    def send_elect_accept(self, defaultPORT: int, acceptorId: int, payload):
        if random.random() > 1.00:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, acceptorId) + "^" * 80)
            return "TimeOut"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', defaultPORT + acceptorId)
        try:
            sock.connect(addr)
            sock.sendall((payload).encode('utf-8'))
            self.log('Proposer #{} Sending elect accept to Acceptor #{}'.format(
                      self.server_id, acceptorId))
            try:
                return sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.log(">>>>>>\nacceptorId: {}\npayload: {}\n<<<<<<<".format(acceptorId, payload))
                raise e
        except ConnectionRefusedError:
            self.log("Acceptor #{} is down, removing it".format(acceptorId))
            self.failed_acceptors.add(acceptorId)
            return "NoAcceptor"
        except socket.timeout as e:
            self.log("socket timeout {}\nAcceptor_id: {}\nCommandInfo: ({}, {})".format(e, acceptorId, clientId, cmdId))
            return "TimeOut"
        finally:
            sock.close()

    def send_command_accept(self, defaultPORT: int, acceptorId: int, clientId: int, cmdId: int, payload):
        if random.random() > 1.00:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, acceptorId) + "^" * 80)
            return "TimeOut"
        rtMessage = ""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', defaultPORT + acceptorId)
        try:
            sock.connect(addr)
            sock.sendall((payload).encode('utf-8'))
            self.log('Proposer #{} Sending command "{}" to Acceptor #{}'.format(
                      self.server_id, payload, acceptorId))
            try:
                rtMessage = sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.log(">>>>>>\nacceptorId: {}\npayload: {}\n<<<<<<<".format(acceptorId, payload))
                rtMessage = "Failed"
                raise e
        except ConnectionRefusedError:
            self.log("Acceptor #{} is down, removing it".format(acceptorId))
            self.failed_acceptors.add(acceptorId)
            rtMessage = "Failed"
        except socket.timeout as e:
            self.log("socket timeout {}\nAcceptor_id: {}\nCommandInfo: ({}, {})".format(e, acceptorId, clientId, cmdId))
            rtMessage = "TimeOut"
        finally:
            sock.close()
            return rtMessage

    def send_command_decide(self, defaultPORT: int, proposerId: int, clientId: int, cmdId: int, payload):
        if random.random() > 1.00:
            self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, proposerId) + "^" * 80)
            time.sleep(1)
            return "TimeOut"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        addr = ('localhost', defaultPORT + proposerId)
        try:
            sock.connect(addr)
            sock.sendall((payload).encode('utf-8'))
            self.log('SlotID: #{}, Proposer #{} Sending decide command {} to Proposer #{}'.format(
                      self.slotId, self.server_id, payload, proposerId))
            try:
                rtMessage = sock.recv(1024).decode('utf-8')
            except ConnectionResetError as e:
                self.log(">>>>>>\nproposerId: {}\npayload: {}\n<<<<<<<".format(proposerId, payload))
                raise e
        except ConnectionRefusedError:
            self.log("Proposer #{} is down, removing it".format(proposerId))
            self.failed_proposers.add(proposerId)
            rtMessage = 'No Proposer'
        except socket.timeout as e:
            self.log("socket timeout {}\nProposer_id: {}\nCommandInfo: ({}, {})".format(e, proposerId, clientId, cmdId))
            rtMessage = 'No Proposer'
        finally:
            sock.close()
            return rtMessage

    def processDecide(self, infos):
        clientId = int(infos[1])
        cmdId = int(infos[2])
        cmdContent = infos[3]
        slotId = int(infos[4])
        # assert slotId == self.slotId, "Proposer%d's Slot ID %d does not equal to Recv Slot ID %d" % (slotId, slotId, self.slotId)
        if self.pastCommands.get((clientId, cmdId), False) is False:
            self.slotId = slotId
            self.pastCommands[(clientId, cmdId)] = (slotId, cmdContent)
        reply_msg = 'stored_' + str(clientId) + '_' + str(cmdId) + '_' + cmdContent
        
        return reply_msg


    # handle a client connection
    def processClient(self, infos):
        clientId = infos[1]
        cmdId = infos[2]
        cmdContent = infos[3]
        
        if self.pastCommands.get((clientId, cmdId), False) is not False:
            self.log("Command ({}, {}, {}) is already executed".format(clientId, cmdId, cmdContent))
            return
        elif self.is_leader() is False:
            self.log("I am not leader, store Command ({}, {}, {}) in futureCommands".format(clientId, cmdId, cmdContent))
            self.futureCommands[(int(clientId), int(cmdId))] = ('_').join(infos)
            return
        else:
            self.log("I am leader, store Command ({}, {}, {}) in futureCommands".format(clientId, cmdId, cmdContent))
            self.futureCommands[(int(clientId), int(cmdId))] = ('_').join(infos)
            self.log("I am leader, send Command ({}, {}, {}) to Acceptors".format(clientId, cmdId, cmdContent))
            send_msg = 'accept_' + str(self.proposal_Number) + '_' + cmdContent + '_' + str(self.server_id)
            tmpRecvs = []
            for acceptorId in range(self.total_nodes):
                if acceptorId not in self.failed_acceptors:
                    tmpRecvs.append(None)
                    tmpRecvs[-1] = self.send_command_accept(
                        defaultPORT=PORT_ACCEPTOR, 
                        acceptorId=acceptorId, 
                        clientId=clientId, 
                        cmdId=cmdId, 
                        payload=send_msg,
                    )
                    self.log("Receive accepted message {} from Acceptor {}".format(tmpRecvs[-1], acceptorId))
            NumAccepted = len([xx for xx in tmpRecvs if isinstance(xx, str) and 'accepted' in xx])
            if NumAccepted < (self.total_nodes // 2) + 1:
                
                self.log("Reply from acceptor is not enough({}), store Command ({}, {}, {}) to futureCommands".format(NumAccepted, clientId, cmdId, cmdContent))
                self.futureCommands[(int(clientId), int(cmdId))] = ('_').join(infos)
                # reply_msg = 'resend_' + str(clientId) + '_' + str(cmdId) + '_' + str(cmdContent) + '_' + str(self.server_id)
            else:
                decide_msg = 'decide_' + str(clientId) + '_' + str(cmdId) + '_' + str(cmdContent) + '_' + str(self.slotId)
                tmpRecvs = []
                self.log("I am leader, send Decide of Command ({}, {}, {}) to Proposers".format(clientId, cmdId, cmdContent))
                for proposerId in range(self.total_nodes):
                    if proposerId != self.server_id and proposerId not in self.failed_proposers:    
                        tmpRecvs.append(None)
                        tmpRecvs[-1] = self.send_command_decide(
                            defaultPORT=PORT_PROPOSER, 
                            proposerId=proposerId, 
                            clientId=clientId, 
                            cmdId=cmdId, 
                            payload=decide_msg,
                        )
                self.processDecide(decide_msg.split('_'))
                for proposerId in self.sendDecideProposers:
                    self.send_command_decide(
                        defaultPORT=PORT_PROPOSER, 
                        proposerId=proposerId, 
                        clientId=clientId, 
                        cmdId=cmdId, 
                        payload=decide_msg,
                    )
                self.slotId += 1
                # NumExecuted = len([xx for xx in tmpRecvs if isinstance(xx, str) and 'stored' in xx])
                # if NumExecuted == (self.total_nodes // 2):
                #     self.futureCommands[(int(clientId), int(cmdId))] = infos.joins('_')
                # else:
                #     if self.pastCommands.get((clientId, cmdId), False) is False:
                #         pastCommandsPair = self.execute(clientId, cmdId, cmdContent)
                #         self.slotId += 1
                #         self.pastCommands[(clientId, cmdId)] = pastCommandsPair
                #     reply_msg = 'proceeded_' + str(clientId) + '_' + str(cmdId) + '_' + self.pastCommands[(clientId, cmdId)][2]
                #     return reply_msg



    def log_progress(self):
        print("*" * 80 + "\n" + self.progress_str() + "\n" + "*" * 80)

    # when receiving a prepare reply
    # def on_prepare_reply (self, reply):
    #     if not self.is_leader() and not self.is_electing:
    #         return # only leader follows up with accept

    #     idx = reply['instance']
    #     # stores info into progress
    #     pg_ref = self.progress[idx]
    #     prep_ref = pg_ref['prepare']
    #     if reply['server_id'] in prep_ref:
    #         assert prep_ref[reply['server_id']] == reply, \
    #                 '{} != {}'.format(prep_ref[reply['server_id']], reply)
    #     prep_ref[reply['server_id']] = reply

    #     # do we receive a majority of replies?
    #     if len(prep_ref) > self.total_nodes / 2:
    #         # find out the highest numbered proposal
    #         hn = -1
    #         hv = ''
    #         for sid, replied_reply in prep_ref.items():
    #             if replied_reply['prep_n'] > hn:
    #                 hn = replied_reply['prep_n']
    #                 hv = replied_reply['prep_value']

    #         assert hn >= 0
    #         reply['command'] = hv
    #         self.send_accept(reply)


        

    


                    
    # send learn messages to all nodes
    # def send_learn (self, data, cmd):
    #     payload = self.init_payload(data)
    #     payload['type'] = T_LEARN
    #     payload['command'] = cmd
    #     replies = self.send_all(payload)
    #     reply = self.check_stale(replies)
    #     if reply is not None:
    #         self.on_learn(reply)
    #         return

    #     payload['server_id'] = self.server_id
    #     self.on_learn(payload)

    # when a learn message is received
    # def on_learn (self, request):
    #     idx = request['instance']
    #     cmd = request['command']

    #     # update states
    #     self.init_states_to(idx)
    #     self.states[idx] = cmd

    #     # update progress
    #     self.init_progress(idx)
    #     self.progress[idx]['command'] = cmd
    #     if 'client' in request:
    #         self.progress[idx]['client'] = request['client']

    #     # execute if possible
    #     self.execute()

    #     # debug
    #     self.log(self.states, verbose=True)
    #     self.log(self.progress, verbose=True)

    # execute the commands
    # def execute(self):
        
    #     for i in range(self.next_exe, len(self.states)):
    #         cmd = self.states[i]
    #         if cmd == S_UNSURE:
    #             break # a hole in the sequence, we can't execute further

    #         self.next_exe += 1

    #         if cmd.startswith(S_ELECT_LEADER_PREFIX):
    #             new_leader_id = int(cmd[len(S_ELECT_LEADER_PREFIX):])
    #             self.log("resetting leader to {}".format(new_leader_id))
    #             self.leader = new_leader_id
    #         elif cmd.startswith("lock_"):
    #             n_lock = int(cmd.lstrip("lock_"))
    #             assert n_lock >= 0, str(n_lock)
    #             assert n_lock < len(self.locks), str(n_lock)
    #             client = self.progress[i]['client']
    #             if self.locks[n_lock] is None:
    #                 self.locks[n_lock] = client
    #         elif cmd.startswith("unlock_"):
    #             n_lock = int(cmd.lstrip("unlock_"))
    #             assert n_lock >= 0, str(n_lock)
    #             assert n_lock < len(self.locks), str(n_lock)
    #             client = self.progress[i]['client']
    #             if self.locks[n_lock] == client:
    #                 self.locks[n_lock] = None
    #         else:
    #             raise Exception("unknown command: " + cmd)

    # insert initial instance if it doesn't exist before
    # def init_progress(self, idx: int):
    #     if not idx in self.progress:
    #         self.progress[idx] = INIT_PROGRESS()
            
    # insert values into states up to idx
    # def init_states_to(self, idx):
    #     end = len(self.states)
    #     if len(self.states) > idx:
    #         return # the idx already exists

    #     for i in range(end, idx + 1):
    #         self.states.append(S_UNSURE) # fill holes with S_UNSURE

    # copy re-usable fields into a new payload object
    # def init_payload (self, payload):
    #     stripped = {
    #         'instance': payload['instance']
    #     }
    #     if 'client' in payload:
    #         stripped['client'] = payload['client']
    #     return stripped

    # send a message to peer and immediately close connection
    # def send_msg(self, defaultPORT: int, target_id: int, payload, recv=True):
    #     if random.random() > 0.95:
    #         self.log("^" * 80 + "\ndropping {} to {}\n".format(payload, target_id) + "^" * 80)
    #         return # randomly drop message
    #     # self.msg_count += 1
    #     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     if target_id == self.leader:
    #         sock.settimeout((self.server_id + random.random()) * 3)
    #     else:
    #         sock.settimeout(1)
    #     sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    #     addr = ('localhost', defaultPORT + target_id)
    #     # payload['source'] = 'server'
    #     # payload['server_id'] = self.server_id
    #     try:
    #         sock.connect(addr)
    #         sock.sendall(json.dumps(payload).encode('utf-8'))
    #         self.log('Paxos #{}, sending {} to {}'.format(payload['instance'],
    #                   payload, target_id))
    #         if recv:
    #             try:
    #                 return json.loads(sock.recv(1024).decode('utf-8'))
    #             except ConnectionResetError as e:
    #                 self.log(">>>>>>\ntarget_id: {}\npayload: {}\n<<<<<<<".format(target_id, payload))
    #                 raise e
    #     except ConnectionRefusedError:
    #         self.log("peer {} is down, removing it".format(addr))
    #         self.failed_nodes.add(target_id)
    #         if target_id == self.leader:
    #             self.log("send_msg to leader {} failed, trigger election".format(addr))
    #             self.elect_leader()
    #     except socket.timeout as e:
    #         self.log("socket timeout {}\ntarget_id: {}\npayload: {}".format(e, target_id, payload))
    #         raise e
    #     finally:
    #         sock.close()

    # send to all followers (every nodes except myself)
    # def send_all (self, payload):
    #     self.log("send_all({})".format(payload))
    #     replies = []
    #     for i in range(1, self.total_nodes + 1):
    #         if not i == self.server_id and not (i in self.failed_nodes):
    #             self.log('send_all from {} to {}'.format(self.server_id, i))
    #             reply = self.send_msg(i, payload)
    #             if reply is not None:
    #                 replies.append((i, reply))
    #     return replies

    # check whether this server is the current leader

    # def notify(self):
    #     with self.handler_lock:
    #         if self.waiting == W_OK:
    #             return
    #         if time.time() > self.waiting_settime + WAITING_TIMEOUT:
    #             if self.waiting == W_WAITING_PREPARE_REPLIES:
    #                 self.send_prepare(*self.wait_args)
    #             elif self.waiting == W_WAITING_ACCEPT_REPLIES:
    #                 self.send_accept(*self.wait_args)
    #             else:
    #                 assert False, "invalid waiting: {}".format(self.waiting)

    # def elect_leader(self):
    #     with self.handler_lock:
    #         if self.leader not in self.failed_nodes:
    #             self.failed_nodes.add(self.leader)
    #         assert not self.is_electing
    #         self.is_electing = True
    #         idx = self.get_next_instance_idx()
    #         data = {
    #             'instance': idx,
    #             'command': S_ELECT_LEADER(self.server_id)
    #         }
    #         assert idx not in self.progress
    #         self.init_progress(idx)
    #         data['proposal_n'] = self.bump_next_proposal_n(idx)
    #         self.send_prepare(data)




# start server
def start (sid, total, recover=False):
    server = LockServer(sid, total, recover=recover)
    heartbeat_server_thr = HeartBeatRecvThread(sid, server)
    heartbeat_server_thr.start()
    heartbeat_sender_thr = HeartBeatSenderThread(server)
    heartbeat_sender_thr.start()
    redirect_sender_thr = RedirectSenderThread(server)
    redirect_sender_thr.start()
    fetch_server_thr = FetchRecvThread(sid, server)
    fetch_server_thr.start()
    heartbeat_checker_thr = HeartBeatCheckerThread(heartbeat_server_thr.server, server)
    heartbeat_checker_thr.start()
    # handle signal
    def sig_handler (signum, frame):
        heartbeat_checker_thr.stopped = True
        heartbeat_sender_thr.stopped = True
        redirect_sender_thr.stopped = True
        heartbeat_checker_thr.join()
        heartbeat_sender_thr.join()
        redirect_sender_thr.join()
        heartbeat_server_thr.server.shutdown()
        heartbeat_server_thr.server.server_close()
        heartbeat_server_thr.join()
        fetch_server_thr.server.shutdown()
        fetch_server_thr.server.server_close()
        fetch_server_thr.join()
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
        print('Usage:\npython paxos_processor.py [server_id] [total_nodes] [-v]')
        exit(0)
    if len(sys.argv) == 4 and "v" in sys.argv[3]:
        VERBOSE = True
    else:
        VERBOSE = False
    if len(sys.argv) == 4 and "r" in sys.argv[3]:
        RECOVER = True
    else:
        RECOVER = False
    print("Start Recover Mode...")
    start(int(sys.argv[1]), int(sys.argv[2]), recover=RECOVER)
