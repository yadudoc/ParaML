import time
import zmq

def producer():
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind("tcp://*:5557")
    # Start your result manager and workers before you start your producers

    terminate = {'action' : 'break'}
    for num in range(0,2000):
        work_message = { 'num' : num }
        zmq_socket.send_json(work_message)
        print("Sending message : ", num)

    zmq_socket.send_json(terminate)

producer()
