from sre_constants import SUCCESS
import threading
import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
import sys
from concurrent import futures
import random
from threading import Timer
import math
import time

class LogMessage:
        def __init__(self, id, term, command):
            self.id = id
            self.term = term
            self.command = command

class RaftServerHandler(pb2_grpc.RaftService):
    def __init__(self, config_dict, id):
        self.term = 0
        self.state = "follower"

        self.timer_count = random.randint(150, 300)

        self.config_dict = config_dict
        self.config_dict['leader'] = "-1"
        self.n = len(self.config_dict) - 1

        self.heartbeat = 50
        self.id = id
        self.votes = 0
        self.voted = False

        self.election_period = False
        self.voted_for = None

        #new replication attributes
        self.logs = []
        self.commit_id = -1
        self.last_applied = -1
        self.values = {}
        #leader replication attributes

        print(f'I am a follower. Term: {self.term}')
        
        self.follower_timer = Timer(self.timer_count/1000, self.become_candidate)  
        self.follower_timer.start()
        self.candidate_timer = None
        self.apply_thread = threading.Thread(target=self.replication_duty)
        self.apply_thread.start()


    def init_timer(self):
        self.timer_count = random.randint(150, 300)    

    def restart_timer(self, function):
        timer = Timer(self.timer_count*10/1000, function)  
        timer.start()
        return timer

    def update_term(self, n):
        self.term = n
        self.voted = False 

    def send_heartbeat(self, ip_and_port, entries, id):
        try:
            new_channel = grpc.insecure_channel(ip_and_port)
            new_stub = pb2_grpc.RaftServiceStub(new_channel)
            
            new_nextId = entries[-1].id + 1 if len(entries) > 0 else self.nextIndex[id] 
            new_matchId = entries[-1].id if len(entries) > 0 else self.matchIndex[id] 
            
            append_entries_response = new_stub.AppendEntries(pb2.AppendEntryRequest(term = self.term, leaaderId = self.id, prevLogIndex = self.nextIndex[id] - 1, prevLogTerm = self.logs[self.nextIndex[id] - 1].term if len(self.logs) != 0 else self.term, entries = entries, leaderCommit = self.commit_id))

            if(append_entries_response.term != -1):
                if(append_entries_response.success == False):
                    self.update_term(append_entries_response.term)
                    print(f'I am a follower. Term: {self.term}')
                    self.become_follower()
                else: 
                    self.nextIndex[id] = new_nextId
                    self.matchIndex[id] = new_matchId                        
        except:
            pass

    def leader_duty(self):
        while self.state == "leader":
            hb_threads = []
            for id, ip_and_port in self.config_dict.items():
                if(id != 'leader' and id != str(self.id)):
                    hb_threads.append(threading.Thread(target=self.send_heartbeat, args=[ip_and_port, [], int(id)]))
            [t.start() for t in hb_threads]
            [t.join() for t in hb_threads]
            time.sleep(50/1000)

    def leader_replication_duty(self):
         while self.state == "leader":
            try:
                hb_threads = []
                for i in range(0, len(self.nextIndex)):
                    if len(self.logs) > self.nextIndex[i] and i != self.id:
                        ip_and_port = self.config_dict[str(i)]
                        entries = []
                        for j in range(self.nextIndex[i], len(self.logs)):
                            entries.append(pb2.Entry(id = self.logs[j].id, termNumber = self.logs[j].term, message = self.logs[j].command))
                        hb_threads.append(threading.Thread(target=self.send_heartbeat, args=[ip_and_port, entries, int(i)]))
                [t.start() for t in hb_threads]
                [t.join() for t in hb_threads]
                time.sleep(50/1000)
            except Exception as e:
                pass

    def leader_commitId_duty(self):
        while self.state == 'leader':
            newCommitId = self.commit_id + 1
            val = 0
            for i in range(0, len(self.matchIndex)):
                if self.matchIndex[i] >= newCommitId or self.id == i:
                    val += 1
            if val >= math.ceil((len(self.config_dict)-1)/2):
                self.commit_id = newCommitId

    def replication_duty(self):
        while self.state != "sleeping":
            if self.commit_id > self.last_applied:
                self.last_applied += 1
                log = self.logs[self.last_applied].command
                val, key = log.split(":")
                self.values[val] = key

    def check_votes(self):
        print('Votes received')

        # закончили голосование
        self.election_period = False
        # затираем голос
        self.voted_for = None


        if self.state != 'candidate':
            return
        if(self.votes >= math.ceil((len(self.config_dict)-1)/2)):
            self.state = 'leader'
            self.config_dict['leader'] = str(self.id)
            self.nextIndex = [len(self.logs) for i in range(0, self.n)]
            self.matchIndex = [-1 for i in range(0, self.n)]
        
            print(f'I am a leader. Term: {self.term}')

            # Вот тут мы вызываем вечную функцию лидера которая шлет сердцебиения
            self.leader_thread = threading.Thread(target=self.leader_duty)
            self.leader_thread.start()
            self.leader_replication_thread = threading.Thread(target=self.leader_replication_duty)
            self.leader_replication_thread.start()
            self.leader_commitId_thread = threading.Thread(target=self.leader_commitId_duty)
            self.leader_commitId_thread.start()
        else:
            self.state = 'follower'

            print(f'I am a follower. Term: {self.term}')


            self.init_timer()
            self.become_follower()

    def get_vote(self, ip_and_port):
        try:
            new_channel = grpc.insecure_channel(ip_and_port)
            new_stub = pb2_grpc.RaftServiceStub(new_channel)

            request_vote_response = new_stub.RequestVote(pb2.RequestVoteRequest(term = self.term, candidateId = self.id, lastLogIndex = len(self.logs) - 1, lastLogTerm = self.logs[-1].term if len(self.logs) != 0  else 0))

            if(request_vote_response.result == True):
                self.votes+=1
        except Exception:
            pass

    def become_candidate(self):
        self.term = self.term+1
        self.voted = False
        self.state = 'candidate' 

        leader_id = self.config_dict['leader']
        if(leader_id != None and int(leader_id)>=0):
            print('Leader is dead')

        print(f'I am a candidate. Term: {self.term}')
        
      
        self.candidate_timer = self.restart_timer(self.check_votes)
        self.votes = 1
        self.voted = True

        # начали выборы (останавливаю их в функции check_votes, хз нужно ли тут тоже это делать)
        self.election_period = True
        # проголосовали за себя любимого
        self.voted_for = self.id
        vote_threads = []
        for id, ip_and_port in self.config_dict.items():
            if(id != 'leader' and id != self.id):
                vote_threads.append(threading.Thread(target=self.get_vote, args=[ip_and_port]))
        [t.start() for t in vote_threads]
        [t.join() for t in vote_threads]

    def become_follower(self):
        self.state = "follower"
        if(self.candidate_timer != None):
                self.candidate_timer.cancel()
                self.candidate_timer = None
        self.follower_timer = self.restart_timer(self.become_candidate)


    def reset_votes(self):
        self.votes = 0

    def restart(self, timer):
        timer.cancel()
        timer.start()    

    def check_last_log_index(self, lastLogIndex, lastLogTerm):
        print(f'{lastLogIndex}, {lastLogIndex}')

        if lastLogIndex < len(self.logs) - 1:
            return False 
        if lastLogIndex != -1 and lastLogIndex == len(self.logs) - 1:
            return self.logs[lastLogIndex].term == lastLogTerm
        return True


    def RequestVote(self, request, context):
        # снова начались выборы
        self.election_period = True
        
        # follower
        if(self.state == 'follower'):
            if(self.follower_timer != None):
                self.follower_timer.cancel()
                self.follower_timer = None
                self.follower_timer = self.restart_timer(self.become_follower)

            if request.term == self.term and self.check_last_log_index(request.lastLogIndex, request.lastLogTerm):
                self.voted = True
                self.voted_for = request.candidateId
                print(f'Voted for node {self.voted_for}')


                return pb2.RequestVoteResponse(term = self.term, result = True)
            elif request.term > self.term and self.check_last_log_index(request.lastLogIndex, request.lastLogTerm):
                self.update_term(request.term)
                self.voted = True 
                self.voted_for = request.candidateId 
                print(f'Voted for node {self.voted_for}')


                return pb2.RequestVoteResponse(term = self.term, result = True)
            else:
                return pb2.RequestVoteResponse(term = self.term, result = False)

        # candidate
        elif(self.state == 'candidate'):
            if(request.term == self.term):
                return pb2.RequestVoteResponse(term = self.term, result = False) 

            elif(request.term > self.term):
                self.update_term(request.term)

                print(f'I am a follower. Term: {self.term}')
                self.become_follower()

                if self.check_last_log_index(request.lastLogIndex, request.lastLogTerm):
                    self.voted = True
                    self.voted_for = request.candidateId
                    print(f'Voted for node {self.voted_for}')

                    return pb2.RequestVoteResponse(term = self.term, result = True)  
                return pb2.RequestVoteResponse(term = self.term, result = False)
            else:
                return pb2.RequestVoteResponse(term = self.term, result = False)

        # leader        
        elif(self.state == 'leader'):
            
            if(request.term > self.term):
                self.update_term(request.term)
                if self.check_last_log_index(request.lastLogIndex, request.lastLogTerm):
                    self.voted = True   
                    self.voted_for = request.candidateId 
                    print(f'Voted for node {self.voted_for}')


                    self.become_follower()

                    return pb2.RequestVoteResponse(term = self.term, result = True)
                return pb2.RequestVoteResponse(term = self.term, result = False)
            else:
                return pb2.RequestVoteResponse(term = self.term, result = False)

        # sleeping
        else:
            return pb2.RequestVoteResponse(term = -1, result = False)        

        # голосование закончилось
        self.election_period = False   
        self.voted_for = None    

    def remove_conflicts_from_log(self, id):
        while len(self.logs) != id:
            self.logs.pop()
     
    def add_entries(self, entries):
        entriesToAppend = []
        for entry in entries:
            if len(self.logs) > entry.id and entry.termNumber != self.logs[entry.id].term:
                self.remove_conflicts_from_log(entry.id)
            if len(self.logs) <= entry.id:
                entriesToAppend.append(entry)
            
        for entry in entriesToAppend:
            self.logs.append(LogMessage(id = entry.id, term = entry.termNumber, command = entry.message))


    def AppendEntries(self, request, context):
        if(self.state == 'sleeping'):
            return pb2.AppendEntriesResponse(term = -1, success = False)    
        elif request.term >= self.term and self.state in ['follower', 'leader', 'candidate']:
            if self.state == "follower":
                if self.follower_timer != None:
                    self.follower_timer.cancel()
                self.follower_timer = None
                self.follower_timer = self.restart_timer(self.become_candidate)

            # Потому что if the Candidate receives the message (any message) with the term number greater than its own, it stops the election and becomes a Follower
            # Или if the Leader receives a heartbeat message from another Leader with the term number greater than its own, it becomes a Follower 
            if self.state in ['leader', 'candidate'] and request.term > self.term:
                self.update_term(request.term)
                print(f'I am a follower. Term: {self.term}')

                self.become_follower()
            
            self.config_dict['leader'] = str(request.leaaderId)
            # If log does not contain an entry at prevLogIndex, return False
            if (len(self.logs) - 1 < request.prevLogIndex):
                return pb2.AppendEntriesResponse(term = self.term, success = False)    
            self.add_entries(request.entries)
            if self.commit_id < request.leaderCommit:
                self.commit_id = min(request.leaderCommit, len(self.logs) - 1)
            return pb2.AppendEntriesResponse(term = self.term, success = True)
        else:
            return pb2.AppendEntriesResponse(term = self.term, success = False)    

    def GetLeader(self, request, context):
        if(self.state == "sleeping"):
            return self.get_leader_response(-1, "Server is sleeping")
        else:    
            if(self.election_period):
                if(self.voted_for == None):
                    return self.get_leader_response(-1, "There is election now and server did not voted yet")
                else:
                    return self.get_leader_response(self.voted_for, self.config_dict[str(self.voted_for)])

            else:
                leader_id = self.config_dict['leader']
                if(leader_id == "-1"):
                    return self.get_leader_response(-1, "There is no leader")
                return self.get_leader_response(int(leader_id), str(self.config_dict[leader_id]))

    def get_leader_response(self, leader_id, address):
        return pb2.GetLeaderResponse(leaderId = int(leader_id), address = str(address))        

    def Suspend(self, request, context):    
        if(self.state == "sleeping"):
            return pb2.SuspendResponse(message = "Already suspending")

        else:        
            if self.follower_timer != None:
                self.follower_timer.cancel()
            if self.candidate_timer != None: 
                self.candidate_timer.cancel()

            prev_state = self.state
            print(f"Sleeping for {request.period} seconds")

            self.state = "sleeping"
            time.sleep(request.period)
            self.apply_thread = threading.Thread(target=self.replication_duty)
            self.apply_thread.start()
            self.become_follower()

    def GetVal(self, request, context):
        if request.key in self.values:
            return pb2.GetValPesponse(success = True, value = self.values[request.key])
        return pb2.GetValPesponse(success = False, value = '-1')

    def SetVal(self, request, context):  
        if self.state == "sleeping":
            return pb2.SetValResponse(success = False) 
        if self.state == 'leader':
            self.logs.append(LogMessage(id = len(self.logs), term = self.term, command=f"{request.key}:{request.value}"))      
            return pb2.SetValResponse(success = True)
        else:
            leader_id = self.config_dict['leader']
            if(leader_id == "-1"):
                return pb2.SetValResponse(success = False) 
            leader_ip_and_port = self.config_dict[leader_id]
            new_channel = grpc.insecure_channel(leader_ip_and_port)
            new_stub = pb2_grpc.RaftServiceStub(new_channel)
            return new_stub.SetVal(pb2.SetValRequest(key = request.key, value = request.value))
            
if __name__ == "__main__":
    id = sys.argv[1]

    config_path = r'config.conf' 
    config_file = open(config_path)
    config_dict = config_file.read().split('\n')

    try:
        config = config_dict[int(id)].split(' ')
    except:
        print('No such id in the config file')    
        exit(0)

    service_config_dict = {}    

    for i in range (len(config_dict)):
        line = config_dict[i].split(' ')
        server_id = line[0]
        ip = line[1]
        port = line[2]
        service_config_dict[server_id] = f'{ip}:{port}' 

    raft_service = RaftServerHandler(service_config_dict, int(id))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftServiceServicer_to_server(raft_service, server)

    ip_and_port = f'{config[1]}:{config[2]}'

    server.add_insecure_port(ip_and_port)
    server.start()
    print(f'The server starts at {ip_and_port}')

    try: 
        server.wait_for_termination()
    except KeyboardInterrupt:
        raft_service.state = 'sleeping'
        print('Termination')    
