import pdb

def log(fun):
    def writelog():
        print('before..')
        fun()
        print('end..')
    
    return writelog
class Ts(object):
    def __call__(self):
        print('called')
    def fund(self):
        print('fund')
@log 
def fun1():
    print('run fun...')

if __name__== "__main__":
    ts = Ts()
    ts()