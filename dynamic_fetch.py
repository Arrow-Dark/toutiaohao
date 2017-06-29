import os
import random
import requests
import json
from bs4 import BeautifulSoup
import time
import sys
import traceback
import threading
import redis
import re
import item_perk
from pymongo import MongoClient
import writer_fetch

def check_type(item_id,user_agent):
    try:
        heads={}
        heads['User-Agent']=user_agent
        url='http://www.toutiao.com/item/'+str(item_id)+'/'
        res = requests.get(url,headers=heads,timeout=15)
        if res.status_code==403:
            print(url,res.status_code)
            print('please test the Ip is blocked!')
            while True:
                minutes=random.randint(1,10)
                time.sleep(minutes*60)
                res = requests.get(source_url, headers=heads, timeout=15)
                if res.status_code==200:
                    break
        if re.match(r'^http://www.toutiao.com/i.*$', res.url) and res.status_code==200:
            bs = BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
            scripts = bs.select('body script')
            if not len(scripts):
                print('Data not caught, please test the Ip is blocked!')
            _len=[]
            for x in scripts:
                _len.append(x.text)
            if not len(_len):
                print('Data not caught, please test the Ip is blocked!')
            try:
                max_index = _len.index(max(_len))
            except ValueError:
               max_index =3
               #print(url,res.status_code)
               traceback.print_exc()
            user_info = scripts[max_index].text.replace(' ', '').split(';\n')
            for i in user_info:
                if re.match(r'^.*vargallery.*$', i.strip()):
                    return 1
            return 0
        if re.match(r'^https://temai.snssdk.com/.*$', res.url):
            return 3
        time.sleep(3)
    except requests.exceptions.ConnectionError:
        print('url=https://www.toutiao.com/item/'+str(item_id)+'/')
        traceback.print_exc()


def parse_dy(datas,user_agent):
    _datas=[]
    for data in datas:
        behot_time=data['create_time']
        if not ('group_id' in data['group'].keys() or 'group_id' in data.keys()):
            continue
        try:
            group_id=str(data['group']['group_id'])
        except KeyError:
            group_id = str(data['group_id'])
        try:
            item_id = str(data['group']['item_id'])
        except KeyError:
            item_id = str(data['item_id'])
        try:
            title = data['group']['title']
        except KeyError:
            title = data['title']
        img_url=''
        comments_count=data['comment_count']
        video_duration_str='0'
        detail_play_effective_count=data['comment_count']+data['digg_count']
        go_detail_count=data['read_count']
        like_count=data['digg_count']
        _type=data['group']['media_type']
        article_genre = ''
        if _type==1:
            _type=check_type(item_id,user_agent)
            if _type == 0:
                article_genre = 'article'
            elif _type == 1:
                article_genre = 'gallery'
            elif _type == 3:
                article_genre = 'other'
        elif _type==2:
            article_genre='video'
        del data
        _data={}
        _data['behot_time']=behot_time
        _data['group_id'] = group_id
        _data['item_id'] = item_id
        _data['title'] = title
        _data['comments_count'] = comments_count
        _data['go_detail_count'] = go_detail_count
        _data['like_count'] = like_count
        _data['img_url'] = img_url
        _data['article_genre'] = article_genre
        _data['video_duration_str'] = video_duration_str
        _data['detail_play_effective_count'] = detail_play_effective_count
        if _data['article_genre']!='video':
            del _data['video_duration_str']
            _datas.append(_data)
        else:
            _datas.append(_data)
    return _datas

def check_exist(items_id,item_id):
    if item_id in items_id:
        return False
    else:
        return True

def fetch_dy_list(uid,pool,user_agent,items_id):
    heads={}
    heads['User-Agent']=user_agent
    articles = []
    gallerys = []
    videos = []
    others = []
    resources_num = {}
    try:
        json_num=1
        url='http://i.snssdk.com/dongtai/list/v9/?user_id='+str(uid)+'&callback=jsonp'+str(json_num)
        res = requests.get(url,headers=heads,timeout=30)
        content=res.content.decode('utf-8').replace(r'\/','/').replace(r'\\u','/u').replace(r'\\','').replace('/u',r'\u').replace('\\\\','\\').replace('false', 'False').replace('true', 'True').replace('null', 'None')
        content=eval(content[7:-1])
        if 'data' in content.keys() and 'data' in content['data'].keys():
            original_data=content['data']['data']
            if len(original_data) != 0:
                rcli = redis.StrictRedis(connection_pool=pool)
                for x in original_data:
                    try:
                        item_id = str(x['group']['item_id'])
                    except KeyError:
                        item_id = str(x['item_id'])
                    behot_time=x['create_time']
                    is_exist=check_exist(items_id,item_id)
                    is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                    if is_again_working and is_exist:
                        #rcli.lpush('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                        rcli.sadd('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                    else:
                        print(uid + '_Dynamic overtime has stopped fetching')
                        return

                '''
                _data=json.loads(rcli.brpop('toutiao_dynamic_original_data')[1].decode())
                uid=_data['uid']
                data_mid=_data['original_data']
                datas = parse_dy(data_mid,user_agent)
                rcli.lpush('toutiao_dynamic_mid_data',{'uid':uid,'mid_data':datas})
                _data=json.loads(rcli.brpop('toutiao_dynamic_mid_data')[1].decode())
                uid=_data['uid']
                datas=_data['mid_data']
                is_again_working = writer_fetch.json_analyze(uid, datas, articles, gallerys, videos,others, pool)
                if is_again_working==-1:
                    resources_num['_id'] = uid
                    resources_num['articles'] = articles
                    resources_num['galleries'] = gallerys
                    resources_num['videos'] = videos
                    resources_num['others'] = others
                    resources_num['crawled_at'] = time.time()
                    print(uid + '_Dynamic overtime has stopped fetching')
                    return resources_num
                '''
                max_cursor=content['data']['max_cursor']
                has_more=content['data']['has_more']
                while has_more:
                    json_num+=1
                    url='http://i.snssdk.com/dongtai/list/v9/?user_id='+str(uid)+'&max_cursor='+str(max_cursor)+'&callback=jsonp'+str(json_num)
                    res=requests.get(url,timeout=30)
                    content = res.content.decode('utf-8').replace('\\\\', '\\').replace('false', 'False').replace('true', 'True').replace('null', 'None')
                    start_index=len('jsonp'+str(json_num)+'(')
                    content = eval(content[start_index:-1])
                    if 'data' in content.keys() and 'data' in content['data'].keys():
                        has_more = content['data']['has_more']
                        max_cursor = content['data']['max_cursor']
                        original_data=content['data']['data']
                        if len(original_data) != 0:
                            rcli = redis.StrictRedis(connection_pool=pool)
                            for x in original_data:
                                try:
                                    item_id = str(x['group']['item_id'])
                                except KeyError:
                                    item_id = str(x['item_id'])
                                behot_time=x['create_time']
                                is_exist=check_exist(items_id,item_id)
                                is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                                if is_again_working and is_exist:
                                    #rcli.lpush('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                    rcli.sadd('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                            #datas = parse_dy(content['data']['data'])                           
                            if not has_more:
                                print(uid + '_Dynamic overtime has stopped fetching')
                                return
                            elif not has_more and len(original_data) != 0:
                                for x in original_data:
                                    try:
                                        item_id = str(x['group']['item_id'])
                                    except KeyError:
                                        item_id = str(x['item_id'])
                                    behot_time=x['create_time']
                                    is_exist=check_exist(items_id,item_id)
                                    is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                                    if is_again_working and is_exist:
                                        #rcli.lpush('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                        rcli.sadd('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                print(uid + '_Dynamic overtime has stopped fetching')
                                return
                            elif has_more and len(original_data) != 0:
                                for x in original_data:
                                    try:
                                        item_id = str(x['group']['item_id'])
                                    except KeyError:
                                        item_id = str(x['item_id'])
                                    behot_time=x['create_time']
                                    is_exist=check_exist(items_id,item_id)
                                    is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                                    if is_again_working and is_exist:
                                        #rcli.lpush('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                        rcli.sadd('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                    else:
                                        print(uid + '_Dynamic overtime has stopped fetching')
                                        return
                    else:
                        has_more =False
    except:       
        traceback.print_exc()

def  parse_dyList(pool,user_agents):
    rcli=redis.StrictRedis(connection_pool=pool)
    agent_lens=len(user_agents)
    while True:
        try:
            articles = []
            gallerys = []
            videos = []
            others = []
            resources_num = {}
            index=(random.randint(0,agent_lens-1))%agent_lens
            user_agent=user_agents[index]
            #dda=rcli.brpop('toutiao_dynamic_original_data')[1].decode('utf-8')
            #print(dda)
            #_data=json.loads(rcli.brpop('toutiao_dynamic_original_data')[1].decode('utf-8'))
            #_data=eval(rcli.brpop('toutiao_dynamic_original_data')[1].decode('utf-8'))
            _data=rcli.spop('toutiao_dynamic_original_data')
            if not _data:
                time.sleep(2)
                continue
            _data=eval(_data.decode('utf-8'))
            uid=_data['uid']
            data_mid=_data['original_data']
            datas = parse_dy(data_mid,user_agent)
            writer_fetch.json_analyze(uid, datas, articles, gallerys, videos,others, pool)
            resources_num['_id'] = uid
            resources_num['articles'] = articles
            resources_num['galleries'] = gallerys
            resources_num['videos'] = videos
            resources_num['others'] = others
            resources_num['crawled_at'] = time.time()
            #print(resources_num)
            perk_item_thread = threading.Thread(target=item_perk.perk_item,args=(resources_num, pool))
            perk_item_thread.start()
            perk_item_thread.join()
            print('Dynamic separation!')
        except:
            traceback.print_exc()