from parsl import App

#@App('python', cache=True)
@App('python')
def hello(items):
    return ["Hello {0}".format(item) for item in items]

#@App('python', cache=True)
@App('python')
def increment(items):
    return [item+1 for item in items]

