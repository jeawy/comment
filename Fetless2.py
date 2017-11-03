# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr
from decorators import timelog

@timelog
def run():
    fetch_new_without_thr(sql="fetched = 1 and counter < 2000 and counter > 1000", fake=True)
if __name__ == "__main__": 
    run()
    
