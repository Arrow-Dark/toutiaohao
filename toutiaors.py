import os
import random
import requests
import urllib
import json
from bs4 import BeautifulSoup
from pymongo import MongoClient
import optparse
import time
import datetime
import sys
import traceback
import threading
import redis
import re


# def start():
#     global INPUT_TIME
#     INPUT_TIME=None
#     global TODAY
#     TODAY=None

#     parser = optparse.OptionParser('usage %prog -g <H|C> -e <EPOCH> -t <TIME>')
#     parser.add_option('-T', dest='input_time', type='string', help='Input Time. eg: 2017-11-12')
#     (options, args) = parser.parse_args()

#     if options.input_time != None:
#         TODAY=options.input_time
#         INPUT_TIME = datetime.datetime.strptime(options.input_time, "%Y-%m-%d")

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
            'crawled_at':int(time.time()*1000) if not INPUT_TIME else int(INPUT_TIME.timestamp()*1000)
            }
            print(uid+'_This user information is captured!')
            return toutiaors
    except:
        traceback.print_exc()


def toutiaors_updated_daily(db,toutiaor):
    try:
        conn=db.toutiaors_daily
        today=time.strftime("%Y-%m-%d", time.localtime()) if not TODAY else TODAY
        _id=toutiaor['_id']+'-'+today
        today=int(time.mktime(time.strptime(today, '%Y-%m-%d'))*1000)
        #conn.update({'_id': _id},{'toutiaor_id':toutiaor['_id'],'follower_count':toutiaor['follower_count'],'fans_count':toutiaor['fans_count'],'date':today}, True)

        data=[{'id':_id,'index_name':'toutiao_logs','type_name':'toutiao_logs','toutiaor_id':toutiaor['_id'],'follower_count':toutiaor['follower_count'],'fans_count':toutiaor['fans_count'],'date':today}]
        requests.post('http://59.110.52.213/stq/api/v1/pa/toutiaoLogs/add',headers={'Content-Type':'application/json'},data=json.dumps(data))
        print(data[0])
    except:
        traceback.print_exc()

def tr2mongo(wirterTitles,db):
    try:
        conn=db.toutiaors
        #time_fetch =time.time()
        uid=wirterTitles['_id']
        name=wirterTitles['name']
        if uid!=None and uid!='' and name!=None and name!='':
            conn.update({'_id': uid}, wirterTitles, True)
            print(uid + '_This user information is stored!')
    except:
        traceback.print_exc()

def worker(rpool,db1,db2,user_agents):
    user_agent=random.choice(user_agents)
    rcli=redis.StrictRedis(connection_pool=rpool)
    ids=list(x.decode() for x in rcli.hkeys('primary_ids_hash'))
    while 1:
        db=db1 if db1.client.is_primary else db2
        uid=ids.pop(0) if len(ids) else None
        if uid:
            toutiaor=fetchWriterTitle(uid,user_agent)
            if rcli.hexists('primary_ids_hash',uid):
                toutiaors_updated_daily(db,toutiaor)
            #tr2mongo(toutiaor,db)
            #print(toutiaor)
        time.sleep(2)




def timeing_job(rpool,db1,db2,user_agents):
    while 1:
        time.sleep(random.randint(0,3600))
        worker(rpool,db1,db2,user_agents)
        time.sleep(12*3600)
        


    
