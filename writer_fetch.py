#!/usr/bin/python3
import os
import random
import requests
import urllib
import json
from bs4 import BeautifulSoup
import time
import datetime
import sys
import traceback
import threading
import redis
import re
import item_perk
import dynamic_fetch
from pymongo import MongoClient
#import Drivers

def fetchWriterTitle(uid,user_agent):
    try:
        heads={}
        heads['User-Agent'] = user_agent
        url_html = 'http://m.toutiao.com/profile/{}/'.format(uid)
        #print(url_html)
        try:
            req=urllib.request.Request(url_html,headers=heads)
            res_u=urllib.request.urlopen(req)
            res_r=None
        except requests.exceptions.Timeout:
            res_u=None
            res_r = requests.get(url_html,headers=heads,timeout=30)
        if (res_u and res_u.getcode()==200) or (res_r and res_r.status==200):
            html=res_u.read().decode('utf-8') if res_u else res_r.content().decode('utf-8')
            bs=BeautifulSoup(html,'html.parser')
            name=bs.select_one('#username').text if bs.select_one('#username') else ''
            introduction=bs.select_one('#description').text if bs.select_one('#description') else ''
            avatar_img=bs.select_one('#userlogo').get('src') if bs.select_one('#userlogo') else ''
            followers=bs.select_one('#followingnum').get('data-num') if bs.select_one('#followingnum') else 0
            fans=bs.select_one('#followernum').get('data-num') if bs.select_one('#followernum') else 0
            toutiaors = {
            '_id':uid,
            'avatar_img': avatar_img,
            'name': name,
            'introduction': introduction,
            'follower_count': int(followers),
            'fans_count': int(fans),
            'crawled_at':int(time.time()*1000)
            }
            print(uid+'_This user information is captured!')
            return toutiaors
    except:
        traceback.print_exc()


def check_time(behot_time,pool,uid):
    rcli = redis.StrictRedis(connection_pool=pool)
    if rcli.hexists('item_AGV_hash',uid):
        item=eval(rcli.hget('item_AGV_hash',uid).decode())
        crawled_at=item['crawled_at']
        can_fech_time=crawled_at-(7*24*60*60)
        #print(time.strftime("%Y-%m-%d",time.localtime(can_fech_time)))
        #can_fech_time=time.mktime(time.strptime('2017-10-28',"%Y-%m-%d"))
        if behot_time>=can_fech_time:
            return 1
        else:
            return 0
    else:
        #can_fech_time=(time.time())-(1*24*60*60)
        can_fech_time=time.mktime(time.strptime('2017-01-01',"%Y-%m-%d"))
        #print(time.strftime("%Y-%m-%d",time.localtime(behot_time)))
        if behot_time>=can_fech_time:
            return 1
        else:
            return 0

def check_phone(_dict):
    if 'data' in _dict.keys():
        return _dict['data']
    else:
        _dict['data'] = {'like_count': 0,'digg_count':0,'bury_count':0}
        return _dict['data']


def str2duration(sTime):
    p1 = "^([0-9]+):([0-5][0-9]):([0-5][0-9])$"
    p2 = "^([0-5][0-9]):([0-5][0-9])$"
    cp1 = re.compile(p1)
    cp2=re.compile(p2)
    try:
        if cp1.match(sTime):
            t = sTime.split(':')
            for i in range(len(t)):
                x = int(t[i])
                t[i] = x
            return 3600 * t[0] + 60 * t[1] + t[2]
        elif cp2.match(sTime):
            t = sTime.split(':')
            for i in range(len(t)):
                x = int(t[i])
                t[i] = x
            return 60 * t[0] + t[1]
        else:
            return 0
    except TypeError:
        return 0



class fetchWriterTitleThread(threading.Thread):
    def __init__(self,uid,user_agent):
        threading.Thread.__init__(self)
        self.uid=uid
        self.user_agent = user_agent
        self.result=None
    def run(self):
        self.result=fetchWriterTitle(self.uid,self.user_agent)
    def get_result(self):
        return self.result

class fetchContentThread(threading.Thread):
    def __init__(self,uid,pool,user_agent):
        threading.Thread.__init__(self)
        self.uid=uid
        self.pool = pool
        self.user_agent=user_agent
        self.result=None
    def run(self):
        self.result=fetchContent(self.uid,self.pool,self.user_agent)
    def get_result(self):
        return self.result


class fetch_dy_thread(threading.Thread):
    def __init__(self,uid,pool,user_agent,items_id,db):
        threading.Thread.__init__(self)
        self.uid=uid
        self.pool = pool
        self.user_agent=user_agent
        self.items_id=items_id
        self.db=db
        self.result=None
    def run(self):
        self.result=dynamic_fetch.fetch_dy_list(self.uid,self.pool,self.user_agent,self.items_id,self.db)
    def get_result(self):
        return self.result


def wirterTitles_into_mongo(wirterTitles,db):
    try:
        conn=db.toutiaors
        #time_fetch =time.time()
        uid=wirterTitles['_id']
        name=wirterTitles['name']
        if uid!=None and uid!='' and name!=None and name!='':
            conn.update({'_id': uid}, wirterTitles, True)
            print(uid + '_This user information is stored!')
    except:
        '''
        with open(os.path.abspath('.') + '/errLogs' + '/wirterTitles_into_mongo_log.txt', 'a', encoding='utf-8',
                  errors='ignore') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) +'_'+wirterTitles['_id']+':\n')
            traceback.print_exc(file=f)
            f.write('------------------------------------------\n\n')
        '''
        traceback.print_exc()


def toutiaors_updated_daily(db,toutiaor):
    try:
        conn=db.toutiaors_daily
        _id=toutiaor['_id']+'-'+time.strftime("%Y-%m-%d", time.localtime())
        conn.update({'_id': _id},{'toutiaor_id':toutiaor['_id'],'follower_count':toutiaor['follower_count'],'fans_count':toutiaor['fans_count']}, True)
    except:
        traceback.print_exc()


def listOfWorks_into_redis(listOfWorks,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        uid=listOfWorks['_id']
        if rcli.hexists('item_AGV_hash',uid):
            item=eval(rcli.hget('item_AGV_hash',uid).decode())
            if listOfWorks['crawled_at']>item['crawled_at']:
                rcli.hset('item_AGV_hash',uid,{'crawled_at':listOfWorks['crawled_at']})
        else:
            rcli.hset('item_AGV_hash',uid,{'crawled_at':listOfWorks['crawled_at']})
    except:
        '''
        with open(os.path.abspath('.') + '/errLogs' + '/listOfWorks_into_redis_log.txt', 'a', encoding='utf-8',
                  errors='ignore') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) +'_'+listOfWorks['_id']+'_'+listOfWorks['item_id']+':\n')
            traceback.print_exc(file=f)
            f.write('------------------------------------------\n\n')
        '''
        traceback.print_exc()

def headlineIds(pool,db1,db2,userAgents):
    print('headlineIds')
    rcli = redis.StrictRedis(connection_pool=pool)
    while True:
        try:
            if rcli.info('memory')['used_memory'] > (700*1024*1024):
                while 1:
                    if rcli.info('memory')['used_memory'] < (200*1024*1024):
                        break
                    else:
                        time.sleep(900)
                        continue
            if db1.client.is_primary :
                db=db1
                db2.client.close()
            elif db2.client.is_primary :
                db = db2
                db1.client.close()
            user_agent=random.choice(userAgents)
            uid = rcli.brpoplpush('toutiao_id_list','toutiao_id_list',0).decode()
            #uid='50371413220'
            print('Geted the user id_'+uid)
            if uid != None:
                toutiaor=fetchWriterTitle(uid,user_agent)
                if rcli.hexists('primary_ids_hash',uid):
                    toutiaors_updated_daily(db,toutiaor)
                wirterTitles_into_mongo(toutiaor,db)
                #dy_thread=threading.Thread(target=dynamic_fetch.fetch_dy_list,args=(uid,pool,user_agent,db))
                dy_thread=fetch_dy_thread(uid,pool,user_agent,items_id,db)
                dy_thread.start()
                dy_thread.join()
                if dy_thread.get_result():
                    listOfWorks_into_redis({'_id':uid,'crawled_at':time.mktime(datetime.date.today().timetuple())},pool)
            #break
            time.sleep(5)
        except:
            traceback.print_exc()
            time.sleep(5)

