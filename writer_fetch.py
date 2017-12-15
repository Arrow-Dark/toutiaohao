#!/usr/bin/python3
import os
import random
import requests
import urllib
import json
from bs4 import BeautifulSoup
import time
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
        avtar_img = ''
        name = ''
        abstract = ''
        guanzhu = 0
        fensi = 0
        url_html = 'http://www.toutiao.com/c/user/' + str(uid) + '/'
        #print(url_html)
        try:
            #res = requests.get(url, timeout=30)
            req=urllib.request.Request(url_html,headers=heads)
            res=urllib.request.urlopen(req)
        except requests.exceptions.Timeout:
            #res = requests.get(url, timeout=30)
            req=urllib.request.Request(url_html,headers=heads)
            res=urllib.request.urlopen(req)
        #print(res_html.status_code)
        if res.status==200:
            #html=res_html.content.decode('utf-8')
            html=res.read().decode('utf-8')
            bs=BeautifulSoup(html,'html.parser')
            #scripts=bs.find(attrs={"type": "text/javascript"})
            scripts=bs.select_one('script[type="text/javascript"]')#[type="text/javascript"]
            #print(scripts)
            script_heads=scripts.text.replace('\n','').replace(' ','').split(';')
            #print(script_heads)
            #time.sleep(3)
            script_statistics=scripts.find_next_sibling().find_next_sibling().text.replace('\n','').replace(' ','').split(';')
            print(script_statistics)
            for attr in script_heads:
                attr=attr.strip()
                if re.match('^.*header=.*$',attr):
                    heads=attr.split('{')[1].split('}')[0].replace('\n','').replace(' ','').replace("'",'').split(',')
                    for head in heads:
                        head=head.strip()
                        if re.match('^avtar_img:.*$',head):
                            avtar_img='http:'+head.split(':')[-1]
                        elif re.match('^name:.*$',head):
                            name = head.split(':')[1]
                        elif re.match('^abstract:.*$',head):
                            abstract = head.split(':')[1]
            for attr in script_statistics:
                attr = attr.strip()
                print(attr)
                if re.match('^.*statistics.*$',attr):
                    statistics=attr.split('{')[1].split('}')[0].replace('\n','').replace(' ','').replace("'",'').split(',')
                    for statistic in statistics:
                        statistic=statistic.strip()
                        if re.match('^guanzhu:.*$',statistic):
                            print(statistic)
                            guanzhu=statistic.split(':')[1]
                        elif re.match('^fensi:.*$',statistic):
                            print(statistic)
                            fensi = statistic.split(':')[1]
            toutiaors = {
            '_id':uid,
            'avatar_img': avtar_img,
            'name': name,
            'introduction': abstract,
            'follower_count': int(guanzhu),
            'fans_count': int(fensi),
            'crawled_at':int(time.time()*1000)
            }
            print(uid+'_This user information is captured!')
            return toutiaors
    except:
        with open(os.path.abspath('.') + '/errLogs' + '/fetchWriterTitle_log.txt', 'a', encoding='utf-8',
                  errors='ignore') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ':\n')
            traceback.print_exc(file=f)
            f.write('------------------------------------------\n\n')
        traceback.print_exc()


def check_time(behot_time,pool,uid):
    rcli = redis.StrictRedis(connection_pool=pool)
    if rcli.hexists('item_AGV_hash',uid):
        item=eval(rcli.hget('item_AGV_hash',uid).decode())
        crawled_at=item['crawled_at']
        #can_fech_time=crawled_at-(15*24*60*60)
        can_fech_time=time.mktime(time.strptime('2017-10-28',"%Y-%m-%d"))
        if behot_time>=can_fech_time:
            return 1
        else:
            return 0
    else:
        #can_fech_time=(time.time())-(15*24*60*60)
        can_fech_time=time.mktime(time.strptime('2017-10-28',"%Y-%m-%d"))
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
        behot_time=data['behot_time']
        global is_again_working
        is_again_working=check_time(behot_time,pool,uid)
        if not is_again_working:
            continue
        genre = data['article_genre']
        if genre == 'article' and ('video_duration_str' not in data.keys()):
            url = 'https://is.snssdk.com/2/article/information/v20/?group_id=' + data['group_id'] + '&item_id=' + data['item_id'] + '&aggr_type=1&context=1&from_category=__all__&article_page=0&iid='+iid+'&device_id='+device_id+'&ac=wifi&app_name=news_article&version_code=605&version_name=6.0.5&device_platform='+device_platform
            print('Start grabbing phone information')
            try:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            except requests.exceptions.Timeout:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            if re.match(r'^https://is.snssdk.com/2/article/information/.*$', res.geturl()):
                text=res.read().decode('utf-8')
                _dict=json_po(text)
                #_dict=json_po(res.content.decode('utf-8'))
                print('The mobile phone information grabs the end')
                article_dict=check_phone(_dict)
                like_count = 0
                if 'digg_count' in article_dict.keys():
                    like_count = article_dict['digg_count']
                elif 'like_count' in article_dict.keys():
                    like_count = article_dict['like_count']
                article = {
                    'group_id': data['group_id'],
                    'item_id': data['item_id'],
                    'title':data['title'],
                    'go_detail_count': data['go_detail_count'],
                    'comments_count': data['comments_count'],
                    'like_count':like_count,
                    'behot_time': data['behot_time'],
                    'article_genre':genre
                }
                articles.append(article)
        elif genre == 'gallery' and ('video_duration_str' not in data.keys()):
            url = 'https://is.snssdk.com/2/article/information/v20/?group_id=' + data['group_id'] + '&item_id=' + data['item_id'] + '&aggr_type=1&context=1&from_category=__all__&article_page=0&iid='+iid+'&device_id='+device_id+'&ac=wifi&app_name=news_article&version_code=605&version_name=6.0.5&device_platform='+device_platform
            print('Start grabbing phone information')
            try:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            except requests.exceptions.Timeout:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            if re.match(r'^https://is.snssdk.com/2/article/information/.*$', res.geturl()):
                text=res.read().decode('utf-8')
                _dict=json_po(text)
                #_dict = json.loads(res.read().decode('utf-8'))
                #_dict=json_po(res.content.decode('utf-8'))
                print('The mobile phone information grabs the end')
                gallery_dict = check_phone(_dict)
                like_count = 0
                if 'digg_count' in gallery_dict.keys():
                    like_count = gallery_dict['like_count']
                elif 'like_count' in gallery_dict.keys():
                    like_count = gallery_dict['like_count']
                gallery = {
                    'group_id': data['group_id'],
                    'item_id': data['item_id'],
                    'title': data['title'],
                    'go_detail_count': data['go_detail_count'],
                    'comments_count': data['comments_count'],
                    'like_count': like_count,
                    'article_genre': genre,
                    'behot_time': data['behot_time']
                }
                galleries.append(gallery)
        elif (genre == 'video' and ('video_duration_str' in data.keys())) or ('video_duration_str' in data.keys()):
            url = 'https://is.snssdk.com/2/article/information/v20/?group_id=' + data['group_id'] + '&item_id=' + data['item_id'] + '&aggr_type=1&context=1&from_category=__all__&article_page=0&iid='+iid+'&device_id='+device_id+'&ac=wifi&app_name=news_article&version_code=605&version_name=6.0.5&device_platform='+device_platform
            print('Start grabbing phone information')
            try:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            except requests.exceptions.Timeout:
                #res = requests.get(url, timeout=30)
                req=urllib.request.Request(url,headers=headers)
                res=urllib.request.urlopen(req)
            if re.match(r'^https://is.snssdk.com/2/article/information/.*$', res.geturl()):
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
                    'digg_count': digg_count,
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
        #print(res_json.status_code)
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


def listOfWorks_into_redis(listOfWorks,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        with rcli.pipeline() as pipe:
            pipe.multi()
            pipe.hset('item_AGV_hash',listOfWorks['_id'],listOfWorks)
            pipe.execute()
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
    rcli = redis.StrictRedis(connection_pool=pool)
    agent_lens=len(userAgents)
    #uids=['56221239740','58562576245','4212960515','52900612083','56156150735']
    #uids=['58562576245']
    #while True:
    while True:
        try:
            '''
            if db1.client.is_primary :
                db=db1
                db2.client.close()
            elif db2.client.is_primary :
                db = db2
                db1.client.close()
            toutiaors_conn = db.toutiaors
            '''
            index=(random.randint(0,agent_lens-1))%agent_lens
            user_agent=userAgents[index]
            uid = rcli.brpoplpush('toutiao_id_list','toutiao_id_list',0).decode()
            #uid=uids.pop()
            print('Geted the user id_'+uid)
            if uid != None:
                '''
                if not toutiaors_conn.find_one({'_id': uid}):
                    wirterTitles = fetchWriterTitle(uid,user_agent)
                    if wirterTitles != None and wirterTitles['_id'] == uid:
                        wirterTitles_into_mongo(wirterTitles, db)
                        print(wirterTitles)
                #check_be=toutiaors_conn.find_one({'_id': uid})
                # if check_be and check_be['fans_count']>=50:
                '''   
                         
                listOfWorks=fetchContent(uid, pool, user_agent)
                #if listOfWorks['_id'] == uid and (listOfWorks['articles']!=[] or listOfWorks['galleries']!=[] or listOfWorks['videos']!=[]):
                if listOfWorks['_id'] == uid :
                    listOfWorks_thread = threading.Thread(target=listOfWorks_into_redis, args=(listOfWorks, pool))
                    items_=listOfWorks['articles']+listOfWorks['galleries']+listOfWorks['videos']
                    items_id=[]
                    for x in items_:
                        if 'item_id' in x.keys():
                            items_id.append(x['item_id'])
                    dy_thread=threading.Thread(target=dynamic_fetch.fetch_dy_list,args=(uid,pool,user_agent,items_id))
                    dy_thread.start()
                    listOfWorks_thread.start()
                    perk_item_thread = threading.Thread(target=item_perk.perk_item,args=(listOfWorks, pool))
                    perk_item_thread.start()
                    listOfWorks_thread.join()
                    perk_item_thread.join()
                    dy_thread.join()
                '''
                if dy_list != None and dy_list['_id'] == uid and (dy_list['articles']!=[] or dy_list['galleries']!=[] or dy_list['videos']!=[] or dy_list['others']!=[]):
                    dy_list_thread = threading.Thread(target=listOfWorks_into_redis, args=(dy_list, pool))
                    dy_list_thread.start()
                    perk_item_thread = threading.Thread(target=item_perk.perk_item,args=(dy_list, pool))
                    perk_item_thread.start()
                    dy_list_thread.join()
                    perk_item_thread.join()
                '''
            time.sleep(2)
        except:
            traceback.print_exc()
