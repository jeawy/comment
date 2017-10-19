import time

def timelog(func):
    def wrapper(*args, **kswargs):
        start = time.ctime()
        func(*args, **kswargs)
        end = time.ctime()
        print(start, end)
    return wrapper