# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr_top
    
if __name__ == "__main__": 
    for i in range(1, 5):
        try:
            fetch_new_without_thr_top(sql="counter <= 100   group by appid", fake=True )
            break
        except :
            continue
 
