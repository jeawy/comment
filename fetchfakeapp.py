# -*- coding:utf-8 -*-

from FetchComment import fetchfakeapp
    
if __name__ == "__main__": 
    for i in range(1, 5):
        try:
            fetchfakeapp(sql=" where hot >= 25", fake = True)
            break
        except :
            continue
 
