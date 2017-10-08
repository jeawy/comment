
class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        
        return cls._instance

class Borg(object):
    _state = {}
    def __new__(cls, *args, **kwargs):
        ob = super(Borg, cls).__new__(cls, *args, **kwargs)
        ob.__dict__ = cls._state
        return ob



def single(cls, *args, **kwargs):
    instances = {}
    def getinstance():
        print(cls)
        if cls not in instances:
            instances[cls]  = cls(*args, **kwargs)
        return instances[cls]
    return getinstance

@single
class My(object):
    a = 1

def test():
    my1 = My()
    my2 = My()
    print(id(my1))
    print(id(my2))
    print (my1.__dict__)
    my1.a = 100
    print(my2.a)

if __name__ == "__main__":
    ls = [2, 4, 8, 7, 13]
    print(map(lambda x: x+2, ls))