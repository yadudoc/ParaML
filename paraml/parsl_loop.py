# Standard imports
import argparse
import pickle
import zmq
import logging

# Parsl imports
import parsl
from parsl.configs.local import localIPP as config

# Custom imports
from app_catalog import APP_LOOKUP_TABLE

class ZmqServer(object):
    """ Server
    """

    def __init__(self, ip_address="*", port=5555):
        context = zmq.Context(1)
        self.server = context.socket(zmq.REP)
        print("Starting server on {}:{}".format(ip_address, port))
        self.server.bind("tcp://{}:{}".format(ip_address, port))

    def send(self, msg):
        return self.server.send(msg)

    def recv(self):
        return self.server.recv()


def server(ip, port, logger):
    '''We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results
    '''
    parsl.load(config)

    server = ZmqServer(ip, port)
    count = 0
    while True:
        msg = server.recv()
        count+=1
        (msg_type, task_id, task_inputs) =  pickle.loads(msg)
        logger.debug("Requesting {} with {}".format(APP_LOOKUP_TABLE[task_id],
                                                    task_inputs))
        fut = APP_LOOKUP_TABLE[task_id](task_inputs)
        reply = fut.result()
        server.send(pickle.dumps(reply))

        if count > 1000:
            logger.debug("Flushing DFK tasks")
            parsl.dfk().tasks = {}
            count = 0

class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass

if __name__ == "__main__" :

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", default=5555, help="Port to launch the server on")
    parser.add_argument("-i", "--interfaces", default="*", help="Interfaces to listen on")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logger = logging.getLogger("DLHub")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        logging.getLogger('DLHub').addHandler(NullHandler())

    logger = logging.getLogger("DLHub")
    logger.debug("Starting server")
    server(args.interfaces, int(args.port), logger=logger)
