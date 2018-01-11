#!/usr/bin/python3

import multiprocessing
from pymongo import MongoClient
import pymongo
import writer_fetch
import threading
import redis
import essay_fetch
import os
import time
import traceback
#import searchKeyWord
from elasticsearch import Elasticsearch
import socket
import random
import dynamic_fetch


def moreWriterFetch(rpool,db1,db2,user_agents):
    thread_list=[]
    for i in range(1):
        t=threading.Thread(target=writer_fetch.headlineIds,args=(rpool,db1,db2,user_agents,))
        thread_list.append(t)
    for th in thread_list:
        th.start()
    # for th in thread_list:
    #     th.join()

def moreFetchEssay(rpool,es,db1,db2,user_agents):
    thread_list = []
    for i in range(4):
        t = threading.Thread(target=essay_fetch.fetch_essay,args=(rpool,es,db1,db2,user_agents,))
        thread_list.append(t)
    for th in thread_list:
        th.start()
    # for th in thread_list:
    #     th.join()

def workingThread(rpool,es,db1,db2,user_agents):
    t1=threading.Thread(target=moreWriterFetch,args=(rpool, db1,db2,user_agents))
    t2 = threading.Thread(target=moreFetchEssay, args=(rpool,es, db1,db2,user_agents))
    t1.start()
    t2.start()
    thread_list = []
    for i in range(8):
        t=threading.Thread(target=dynamic_fetch.parse_dyList, args=(rpool,user_agents))
        thread_list.append(t)
    for th in thread_list:
        th.start()

def check_start(pool):
    rcli = redis.StrictRedis(connection_pool=pool)
    while True:
        try:
            rcli.brpoplpush('_ping','_pang',0)
            hostName = socket.gethostname()
            print(hostName+':Get the uid fetch privilege, ready to grab the uid!')
            searchKeyWord.cur_tab_url_open(pool)
            rcli.rpoplpush('_pang', '_ping')
            print(hostName + ':End fetching uid, return uid fetch permissions!')
            time.sleep(3)
        except redis.exceptions.ConnectionError:
            print(hostName + ':Disconnect from redis, suggest repair links and restart crawlers!')
            traceback.print_exc()
        except:
            rcli.rpoplpush('_pang','_ping')
            print(hostName + ':Grab uid exception and return uid fetch privileges!')
            traceback.print_exc()


def check_ball(pool):
    rcli = redis.StrictRedis(connection_pool=pool)
    count=0
    while True:
        if rcli.llen('_pang'):
            count+=1
            if count>=20:
                rcli.rpoplpush('_pang', '_ping')
        else:
            count=0
        guess=random.randint(1,11)
        time.sleep(guess)


def pro_pool_():
    with open(os.path.abspath('.') + '/Redis_Mongo_Es' + '/redis_mongo_es.txt', 'r', encoding='utf-8') as f:
        line=f.read()
    with open(os.path.abspath('.') + '/UserAgent' + '/user_agent.txt', 'r', encoding='utf-8') as u:
        user_agents=u.read().split('\n')[0:-1]
    _dict = eval(line)
    red_dict = _dict['red']
    mon_dict = _dict['mon1']
    mon_dict2 = _dict['mon2']
    es_dict = _dict['es']
    red_host = red_dict['host']
    red_port = int(red_dict['port'])
    red_pwd = red_dict['password']
    mon_host = mon_dict['host']
    mon_port = str(mon_dict['port'])
    mon_user = mon_dict['user']
    mon_pwd = mon_dict['password']
    mon_dn = mon_dict['db_name']
    mon2_host = mon_dict2['host']
    mon2_port = str(mon_dict2['port'])
    mon2_user = mon_dict2['user']
    mon2_pwd = mon_dict2['password']
    mon2_dn = mon_dict2['db_name']
    es_url=es_dict['url']
    es_port=es_dict['port']
    es_name=es_dict['name']
    es_pwd=es_dict['password']
    mon_url='mongodb://' + mon_user + ':' + mon_pwd + '@' + mon_host + ':' + mon_port +'/'+ mon_dn+'?maxPoolSize=8'
    mon_url2 = 'mongodb://' + mon2_user + ':' + mon2_pwd + '@' + mon2_host + ':' + mon2_port + '/' + mon2_dn+'?maxPoolSize=8'
    rpool = redis.ConnectionPool(host=red_host, port=red_port,password=red_pwd)
    #rpool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    #rpool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    es = Elasticsearch([es_url], http_auth=(es_name, es_pwd), port=es_port)
    #es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    #mcli = MongoClient('127.0.0.1', 27017)
    mcli = MongoClient(mon_url,)
    mcli2 = MongoClient(mon_url2)
    db1 = mcli.get_database('toutiao')
    db2 = mcli2.get_database('toutiao') 
    working_thread = threading.Thread(target=workingThread, args=(rpool, es, db1,db2, user_agents))
    working_thread.start()
    working_thread.join()

if __name__=='__main__':
    try:
        pro_pool_()
    except:
        with open(os.path.abspath('.') + '/errLogs' + '/hearlineFetcher_err_log.txt', 'a', encoding='utf-8',
                  errors='ignore') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+':\n')
            traceback.print_exc(file=f)
            f.write('------------------------------------------\n\n')
            traceback.print_exc()
