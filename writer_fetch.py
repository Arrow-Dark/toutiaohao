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
        can_fech_time=crawled_at-(3*24*60*60)
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


def json_po(_str):
    #print(_str)
    sa=_str.replace(r'\"',"'").replace(r'\n','').replace(r'\t','')
    #sa_f=list(x for x in sa if not re.match(r'^.*"open_page_url": "sslocal:',x) and not re.match(r'^.*"context":',x) and not re.match(r'^.*"script":',x) and not re.match(r'^"is.snssdk.com"',x) and not re.match(r'^.*"log_extra"',x) and not re.match(r'^.*"req_id"',x) and not re.match(r'^{*"impr_id"',x) and not re.match(r'^.*"ac=wifi',x) and not re.match(r'^.*ame=',x) and not re.match(r'"ignore_web_transform"',x))
    #sa_e=', '.join(sa_f)
    return json.loads(sa)

def json_analyze(uid,datas,articles,galleries,videos,others,pool,user_agent):
    print('into json_analyze!')
    with open(os.path.abspath('.') + '/PhoneID' + '/phone_id.txt', 'r', encoding='utf-8') as f:
        line=f.read()
    id_dict = eval(line)
    headers = { 'User-Agent' : user_agent }
    iid='9198954949'
    device_id='35699373946'
    device_platform='android'
    if 'iid' in id_dict.keys() and id_dict['iid']!='':
        iid=id_dict['iid']
    if 'device_id' in id_dict.keys() and id_dict['device_id']!='':
        device_id=id_dict['device_id']
    if 'device_platform' in id_dict.keys() and id_dict['device_platform']!='':
        device_platform=id_dict['device_platform']

    for data in datas:   
        #print('json_analyze',data)    
        behot_time=data['behot_time']
        global is_again_working
        is_again_working=check_time(behot_time,pool,uid)
        # if not is_again_working:
        #     continue
        genre = data['article_genre']
        if genre == 'article':
            
            article = {
                'group_id': data['group_id'],
                'item_id': data['item_id'],
                'title':data['title'],
                'go_detail_count': data['go_detail_count'],
                'comments_count': data['comments_count'],
                #'like_count':like_count if ,
                'like_count':data['like_count'],
                'behot_time': data['behot_time'],
                'article_genre':genre
            }
            
            articles.append(article)
        elif (genre == 'video'):
            url = 'https://is.snssdk.com/2/article/information/v20/?group_id=' + data['group_id'] + '&item_id=' + data['item_id'] + '&aggr_type=1&context=1&from_category=__all__&article_page=0&iid='+iid+'&device_id='+device_id+'&ac=wifi&app_name=news_article&version_code=605&version_name=6.0.5&device_platform='+device_platform
            print('Start grabbing phone information')
            try:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            except :
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            if re.match(r'^http.*://is.snssdk.com/2/article/information/.*$', res.geturl()):
                #_dict = json.loads(res.content.decode('utf-8'))
                print(url==res.geturl())
                text=res.read().decode('utf-8')
                print('text_length:',len(text))
                _dict=json_po(text)
                print('The mobile phone information grabs the end')
                video_dict = check_phone(_dict)
                digg_count=0
                bury_count=0
                if 'digg_count' in video_dict.keys():
                    digg_count=video_dict['digg_count']
                elif 'like_count' in video_dict.keys():
                    digg_count =video_dict['like_count']
                if 'bury_count' in video_dict.keys():
                    bury_count = video_dict['bury_count']
                str_duration=data['video_duration_str']
                second_duration=str2duration(str_duration)

                video = {
                    'group_id':data['group_id'],
                    'item_id': data['item_id'],
                    'title': data['title'],
                    'img_url':(data['image_url'] if ('image_url' in data.keys()) else ''),
                    'video_duration_str': second_duration,
                    'detail_play_effective_count': data['detail_play_effective_count'],
                    'comments_count': data['comments_count'],
                    'digg_count': data['like_count'] if 'like_count' in data.keys() else video_dict['digg_count'] if 'digg_count' in video_dict.keys() else video_dict['like_count'] if 'like_count' in video_dict.keys() else 0,
                    'bury_count': bury_count,
                    'behot_time': data['behot_time'],
                    'article_genre': genre
                }
                videos.append(video)
        elif genre == 'other':
            others.append(data)
    time.sleep(1)
    if not is_again_working:
        return -1

def fetchContent(uid,pool,user_agent):
    heads={}
    heads['User-Agent']=user_agent
    articles = []
    gallerys = []
    videos = []
    others=[]
    resources_num={}
    max_behot_time=0
    while True:
        url_json = 'http://www.toutiao.com/c/user/article/?page_type=1&user_id=' + uid + '&max_behot_time=' + str(
            max_behot_time) + '&count=20'
        try:
            res_json = requests.get(url_json, headers=heads, timeout=30)
        except requests.exceptions.Timeout:
            res_json = requests.get(url_json, headers=heads, timeout=30)
        print('fetchContent',res_json.status_code)
        if res_json.status_code == 200:
            try:
                if max_behot_time == 0:
                    try:
                        cook = res_json.headers['Set-Cookie'][:106]
                    except:
                        cook =''
                    heads['Cookie'] = cook
                json_dict = json.loads(res_json.content.decode('utf-8'))
                if 'data' not in json_dict:
                    print('please test the Ip is blocked!')
                    _count=0
                    while True:
                        _count+=1
                        minutes=random.randint(1,10)
                        time.sleep(minutes*60)
                        res = requests.get(url_json, headers=heads, timeout=30)
                        json_dict = json.loads(res.content.decode('utf-8'))
                        if res.status_code==200 and 'data' in json_dict:
                            break
                        if _count>=3:
                            break
                datas = json_dict['data']
                has_more = json_dict['has_more']
                if not has_more and len(datas) == 0:
                    break
                elif has_more and len(datas) == 0:
                    break
                elif not has_more and len(datas) != 0:
                    json_analyze(uid,datas, articles, gallerys, videos,others,pool,user_agent)
                    break
                elif has_more and len(datas) != 0:
                    is_again_working=json_analyze(uid,datas, articles, gallerys, videos,others,pool,user_agent)
                    if is_again_working==-1:
                        break
                    max_behot_time = json_dict['next']['max_behot_time']
            except:
                with open(os.path.abspath('.') + '/errLogs' + '/fetchContent_log.txt', 'a', encoding='utf-8',
                          errors='ignore') as f:
                    f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ':\n')
                    traceback.print_exc(file=f)
                    f.write('------------------------------------------\n\n')
                    traceback.print_exc()
                break
            time.sleep(1)
        else:
            return
    resources_num['_id'] = uid
    resources_num['articles'] = articles
    resources_num['galleries'] = gallerys
    resources_num['videos'] = videos
    resources_num['others'] = others
    resources_num['crawled_at'] = time.time()
    print(uid + '_This user content information is captured!')
    print(uid,len(articles),len(gallerys),len(videos),len(others))
    return resources_num


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


def wirterTitles_into_mongo(wirterTitles,db):
    try:
        conn=db.toutiaors
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
            if db1.client.is_primary :
                db=db1
                db2.client.close()
            elif db2.client.is_primary :
                db = db2
                db1.client.close()
            user_agent=random.choice(userAgents)
            uid = rcli.brpoplpush('toutiao_id_list','toutiao_id_list',0).decode()
            #uid = rcli.brpoplpush('toutiao_id_list','toutiao_id_list_bck',0).decode()
            print('Geted the user id_'+uid)
            if uid != None:
                toutiaor=fetchWriterTitle(uid,user_agent)
                wirterTitles_into_mongo(toutiaor,db)
                items_id=[]
                dy_thread=threading.Thread(target=dynamic_fetch.fetch_dy_list,args=(uid,pool,user_agent,items_id,db))
                dy_thread.start()
                dy_thread.join()
                listOfWorks_into_redis({'_id':uid,'crawled_at':time.mktime(datetime.date.today().timetuple())},pool)
            time.sleep(5)
        except:
            traceback.print_exc()
