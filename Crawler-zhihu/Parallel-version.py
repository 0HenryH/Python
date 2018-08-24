from bs4 import BeautifulSoup
import requests as rq
import json
import multiprocessing
from math import ceil
import multiprocessing, threading, time
import queue
from pybloom_live import BloomFilter
import sqlite3
# import random
header = {
    'User-Agent': , # 自行设置
    'Cookie': #自行设置
}




def scan_page(urlToken,task_q,q):
    url = 'https://www.zhihu.com/api/v4/members/%s/followees?include=data%%5B*%%5D.answer_count%%2Carticles_count%%2Cgender%%2Cfollower_count%%2Cis_followed%%2Cis_following%%2Cbadge%%5B%%3F(type%%3Dbest_answerer)%%5D.topics&offset=0&limit=20'%(urlToken)
    rs = rq.get(url,headers = header)
    try:
        num = rs.json()['paging']['totals']
    except:
        pass
        return [urlToken]
    a = 0
    b = 20
    time.sleep(3)
    for _ in range(1,ceil(num/20)+1): 
        url = 'https://www.zhihu.com/api/v4/members/%s/followees?include=data%%5B*%%5D.answer_count%%2Carticles_count%%2Cgender%%2Cfollower_count%%2Cis_followed%%2Cis_following%%2Cbadge%%5B%%3F(type%%3Dbest_answerer)%%5D.topics&offset=%s&limit=%s'%(urlToken,str(a),str(b))
        rs = rq.get(url,headers = header)
        time.sleep(3)
        try:
            data_json = rs.json()['data']
        except:
            pass
            task_q.put(urlToken)
            return [urlToken]
        a += 20
        b += 20
        # data_json中包含n个字典，每个字典中都有'url_token'
        for item in data_json:
            if not (item['url_token'] in bloom):
                q.put(item['url_token'])
                task_q.put(item['url_token'])
                mylock2.acquire(10)
                bloom.add(urlToken)
                mylock2.release()

def worker1(task_q,q):
    while True:
        try:
            urlToken = task_q.get(100)
        except task_q.Empty:
            break
        scan_page(urlToken,task_q,q)

def handle_data(urlToken):
    url = 'http://www.zhihu.com/people/%s/activities'%(urlToken)
    rs = rq.get(url,headers = header)
    time.sleep(0.075)
    html = rs.text
    try:
        bs = BeautifulSoup(html,'html.parser')
        div_tag = bs.find('div',attrs = {'id':'data'})
        data = json.loads(div_tag['data-state'])
        data = data['entities']['users'][urlToken]
    except:
        pass
        print('error: '+str(urlToken)+" 1")
        return handle_data
    try:
        name = data['name']
    except:
        pass
        print('error: '+str(urlToken)+" 2")
        return handle_data
    try:
        location = data['locations'][0]['name']
    except:
        location = "用户未设置"
    try: 
        followerCount = data['followerCount']
        gender = data['gender']
        voteupCount = data['voteupCount']
        favoritedCount = data['favoritedCount']
        mylock1.acquire(10)
        user_db = sqlite3.connect('D:/users.db')
        task = 'insert into user values("%s","%s","%s",%s,%s,%s,"%s")'%(str(urlToken),str(name),str(gender),str(followerCount),str(voteupCount),str(favoritedCount),str(location))
        user_db.execute(task)
        user_db.commit()
        user_db.close()
        mylock1.release()
        print(name)
    except:
        pass
        print('error: '+str(urlToken)+" 3")
        return handle_data

def worker2(queue):
    while True:
        try:
            urlToken = queue.get(timeout = 10000)
        except queue.Empty:
            break
        handle_data(urlToken)


def master(start):
    task_q = queue.Queue()
    url_queue = queue.Queue() # pylint: disable=E1101
    url_queue.put(start)
    task_q.put(start)
    threads = [
        threading.Thread(target = worker1,args = [task_q,url_queue,]),
        threading.Thread(target = worker2,args = [url_queue,])
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

if __name__ == "__main__":
    conn = sqlite3.connect('D:/users.db')
    create = 'Create TABLE user(url_token TEXT PRIMARY KEY,name TEXT,gender TEXT,followerCount INT,voteupCount INT,favoritedCount INT,location TEXT)'
    conn.execute(create)
    conn.commit()
    conn.close()
    bloom = BloomFilter(capacity=5000000, error_rate=0.001)
    mylock1 = threading.Lock()
    mylock2 = threading.Lock()
    master('excited-vczh')