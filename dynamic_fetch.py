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
from pymongo import MongoClient
import writer_fetch


def parse_jsonp(jsonp_str):
    try:
        return re.search('^[^(]*?\((.*)\)[^)]*$', jsonp_str).group(1)
    except:
        raise ValueError('Invalid JSONP')


def parse_dy(datas,user_agent):
    _datas=[]
    for data in datas:
        behot_time=data['create_time']
        # if not ('group_id' in data['group'].keys() or 'group_id' in data.keys()):
        #     continue
        group_id=str(data['group']['group_id']) if 'group_id' in data['group'].keys() else data['group_id'] if 'group_id' in data.keys() else data['id_str']
        
        item_id = data['item_id_str'] if 'item_id_str' in data else data['id_str']
        
        title = data['group']['title'] if 'title' in data['group'].keys() else data['title'] if 'title' in data.keys() else data['content']
        img_url=''
        comments_count=data['comment_count']
        video_duration_str='0'
        #detail_play_effective_count=data['comment_count']+data['digg_count']
        detail_play_effective_count=data['read_count']
        go_detail_count=data['read_count']
        like_count=data['digg_count']
        _type=data['group']['media_type'] if 'media_type' in data['group'].keys() else 1
        article_genre = ''
        if _type==1:
            article_genre='article'
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

def fetch_dy_list(uid,pool,user_agent,items_id):
    print(uid,'start fetch_dy_list!')
    heads={}
    heads['User-Agent']=user_agent
    articles = []
    gallerys = []
    videos = []
    others = []
    resources_num = {}
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        json_num=1
        max_cursor=0
        has_more=True
        con=0
        while has_more:
            url='http://i.snssdk.com/dongtai/list/v9/?user_id={uid}&max_cursor={cursor}&callback=jsonp{num}'.format(uid=uid,cursor=max_cursor,num=json_num)
            res=requests.get(url,headers=heads,timeout=30)
            content = res.content.decode('utf-8')
            content = parse_jsonp(content)
            content = json.loads(content)
            if 'data' in content.keys() and 'data' in content['data'].keys():
                has_more = content['data']['has_more']
                max_cursor = content['data']['max_cursor']
                original_data=content['data']['data']
                if len(original_data) != 0:
                    for x in original_data:
                        if re.match(r'^http.//toutiao.com/i.*$',x['share_url']) or re.match(r'^http.//weitoutiao.zjurl.cn/.*$',x['share_url']):
                        #if not re.match(r'^http.*//toutiao.com/dongtai.*$',x['share_url']):
                            #print(x['share_url'])
                            try:
                                item_id = x['item_id_str'] if 'item_id_str' in x else x['id_str']
                            except KeyError:
                                continue
                            behot_time=x['create_time']
                            print('create_time:',time.strftime("%Y-%m-%d",time.localtime(behot_time)))
                            #is_exist=False if item_id in items_id else True
                            is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                            #print('is_again_working',is_again_working)
                            if is_again_working:# and is_exist:
                                rcli.sadd('toutiao_dynamic_original_data',{'uid':uid,'original_data':[x]})
                                con+=1
                            else:
                                has_more=False
                                break
                    print(uid,con,'page:',json_num)
                if not has_more:
                    #print(uid + '_Dynamic overtime has stopped fetching')
                    return
            else:
                has_more =False
            json_num+=1
            time.sleep(2)
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
            user_agent=random.choice(user_agents)
            _data=rcli.spop('toutiao_dynamic_original_data')
            if not _data:
                time.sleep(3)
                continue
            _data=eval(_data.decode('utf-8'))
            uid=_data['uid']
            data_mid=_data['original_data']
            datas = parse_dy(data_mid,user_agent)
            writer_fetch.json_analyze(uid, datas, articles, gallerys, videos,others, pool,user_agent)
            resources_num['_id'] = uid
            resources_num['articles'] = articles
            resources_num['galleries'] = gallerys
            resources_num['videos'] = videos
            resources_num['others'] = others
            resources_num['crawled_at'] = time.time()

            perk_item_thread = threading.Thread(target=item_perk.perk_item,args=(resources_num, pool))
            perk_item_thread.start()
            perk_item_thread.join()
            #print('Dynamic separation!')
        except:
            traceback.print_exc()