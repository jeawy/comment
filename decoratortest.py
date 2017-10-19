import pdb

def log(level):
    def wraper(func):
        def writelog():
            print('before..')
            func()
            print('end..')
        
        return writelog
    return wraper
class Ts(object):
    def __init__(self, level):
        pass
    def __call__(self, func):
        def wraper(*args, **kwargs):    
            print('called')
            func(*args, **kwargs)

        return wraper

    def fund(self):
        print('fund')

@Ts('info')
def fun1(ss):
    print('run fun...')

if __name__== "__main__":
    fun1('test')