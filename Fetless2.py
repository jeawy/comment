# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr
from decorators import timelog

@timelog
def run():
    #fetch_new_without_thr(sql="fetched = 3 and  counter > 2000", fake=True)
    fetch_new_without_thr(sql="url is null", fake=True)
if __name__ == "__main__": 
    run()
    
