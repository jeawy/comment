# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr_top
    
if __name__ == "__main__": 
    fetch_new_without_thr_top(sql="counter <= 30  group by appid order by counter desc", fake=True )
          
