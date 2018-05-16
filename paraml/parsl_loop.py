import threading
import time
from parsl import DataFlowKernel, App
from localDockerIPP import localDockerIPP as config

dfk = DataFlowKernel(config=config)

@App('python', dfk)
def hello(items):
    return ["Hello {0}".format(item) for item in items]

@App('python', dfk)
def increment(items):
    return [item+1 for item in items]

def as_thread(fn):

    def run(*args, **kwargs):
        kill_event = threading.Event()
        kwargs['kill_event'] = kill_event
        thread = threading.Thread(target=fn,
                                  name=fn.__name__+".thread",
                                  args=args,
                                  kwargs=kwargs)
        thread.start()
        return thread, kill_event

    return run

#@as_thread
def reply_loop(config, kill_event=None):
    '''We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results
    '''
    while True:
        print("[REPLY] sleeping")
        try:
            die = kill_event.wait(1)
            if die:
                print("[REPLY] got die message")
                return
        except Exception as e:
            print("[REPLY] nothing so far", e)

#@as_thread
def listen_loop(config, kill_event=None):
    '''We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results
    '''

    while True:
        print("[LISTENER] sleeping")
        try:
            die = kill_event.wait(1)
            if die:
                print("[LISTENER] got die message")
                return
        except Exception as e:
            print("[LISTENER] nothing so far", e)

if __name__ == "__main__" :

    items = [ hello([i]) for i in range(0,10)]
    print([i.result() for i in items])
    #listen_loop()
    exit(0)

    listener_thread, listener_kill_event = listen_loop({})
    reply_thread, reply_kill_event = reply_loop({})
    
    time.sleep(4)
    listener_kill_event.set()
    listener_thread.join()

    reply_kill_event.set()
    reply_thread.join()
