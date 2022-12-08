import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

class Client:
    def __init__(self):
        self.channel = None
        self.stub = None

    def connect(self, ip_and_port):
        try:
            self.channel = grpc.insecure_channel(ip_and_port)
            self.stub =pb2_grpc.RaftServiceStub(self.channel)

        except Exception as e:
            print('Server is suspending')    

    def get_leader(self):
        try:
            response = self.stub.GetLeader(pb2.EmptyRequest())
            if(response.leaderId == -1):
                print(response.address)
            else:    
                print(f'{response.leaderId} {response.address}')   
        except Exception as e:
            print(e)          

    def suspend(self, period: int):
        try:
            response = self.stub.Suspend(pb2.SuspendRequest(period = period))  

            if(response.message == 'Alredy suspending'):
                print(response.message)  
        except Exception as e:
            print(e)   

    def getval(self, key):
        try:
            response = self.stub.GetVal(pb2.GetValRequest(key = key))    

            if(response.success == True):
                print(response.value)
            else:
                print('None')    

        except Exception as e:
            print(e)  

    def setval(self, key, value):
        try:
            response = self.stub.SetVal(pb2.SetValRequest(key = key, value = value))    
            if response.success:
                print("all OK!")
            else:
                print("failed")
        except Exception as e:
            print(e)


    def quit(self):
        print('The client ends')
        exit(0)    


    def main_function(self):
        try:
            while True:
                user_input = input()
                command = user_input.split(' ')

                # connection
                if(command[0] == "connect"):
                    self.connect(f'{command[1]}:{command[2]}')

                # get leader        
                elif(command[0] == "getleader"):
                    self.get_leader()

                # suspend
                elif(command[0] == "suspend"):
                    self.suspend(int(command[1]))

                # getval
                elif(command[0] == "getval"):
                    self.getval(command[1])    

                # quit
                elif(command[0] == "quit"):
                    self.quit()

                # getval
                elif(command[0] == "getval"):
                    self.getval(command[1])    

                # setval
                elif(command[0] == "setval"):
                    self.setval(command[1], command[2])    

                # man?
                else:
                    print("Unknown command")        

        except KeyboardInterrupt:
            print("Terminating")   


if __name__ == "__main__":
    print('The client starts')
    client = Client()
    client.main_function()