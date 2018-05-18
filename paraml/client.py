# Standard imports
import zmq
import time
import uuid
import pickle
import argparse

class Client(object):

    def __init__ (self, server_ip="localhost", server_port=5555):

        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://{}:{}".format(server_ip, server_port))

    def request(self, msg):

        self.socket.send(msg)
        reply = self.socket.recv()
        return reply



def run_test(client, N=1000):
    start = time.time()
    for i in range(0,N):
        obj = ( 1,          # 1<- Pred.task 0<-Control.Task
                1,          # Task_Unique ID in app registry
                range(0,5), # inputs (Should be a list,iterator)
               )
        msg = client.request(pickle.dumps(obj))
        #print("Got reply :", pickle.loads(msg))

    delta = time.time() - start
    print("Iters: {} Time_taken: {} Rate: {}/s".format(N, delta, float(N)/delta))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=5555, help="Port to launch the server on")
    parser.add_argument("-a", "--address", default="localhost", help="IP Address to connect to. Default: localhost")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    client = Client(server_ip=args.address,
                    server_port=args.port)
    run_test(client, N=1000)
    #run_test(client, N=100000)
    #run_test(client, N=1000000)

