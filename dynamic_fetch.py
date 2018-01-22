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

def put2es(item,pool,db):
    rcli = redis.StrictRedis(connection_pool=pool)
    index_name='toutiao_articles_and_users'
    type_name='toutiao_articles_and_users'

    uid=item['uid']
    toutiaor=db.toutiaors.find_one({'_id':uid})
    raw=item['original_data']
    info_action = {}
    info_action['index_name']= index_name
    info_action['type_name']= type_name
    info_action['toutiaor_id']= uid
    info_action['id']=raw['item_id_str'] if 'item_id_str' in raw else raw['id_str']
    info_action['title']= raw['group']['title'] if 'title' in raw['group'].keys() else raw['title'] if 'title' in raw.keys() else raw['content']
    info_action['content']= info_action['title']
    info_action['images']=[]
    info_action['labels']= []
    info_action['read_count']=raw['read_count']
    info_action['comment_count']=raw['comment_count']
    info_action['up_count']= raw['digg_count']
    info_action['down_count']=0
    info_action['published_at']= raw['create_time']*1000
    info_action['crawled_at']=int(time.time()*1000)
    info_action['type']= raw['group']['media_type'] if ('media_type' in raw['group'].keys() and raw['group']['media_type']==2) else 0
    info_action['duration']= 0
    info_action['avatar_img']=raw['user']['avatar_url']
    info_action['name']=raw['user']['screen_name']
    info_action['introduction']=toutiaor['introduction']
    info_action['follower_count']=toutiaor['follower_count'] if toutiaor else 0
    info_action['fans_count']=toutiaor['fans_count'] if toutiaor else 0
    info_action['is_finish']='false' if info_action['type']==0 else 'true'
    rcli.lpush('item_ES_list', info_action)
    if info_action['type']==0:
        rcli.lpush('item_AGV_list', info_action)


def fetch_dy_list(uid,pool,user_agent,db):
    print(uid,'start fetch_dy_list!')
    heads={}
    heads['User-Agent']=user_agent
    heads['Accept']='*/*'
    heads['Accept-Encoding']='gzip, deflate, sdch'
    heads['Accept-Language']='zh-CN,zh;q=0.8'
    heads['Connection']='keep-alive'
    heads['Host']='i.snssdk.com'
    heads['Referer']='http://m.toutiao.com/profile/{}/'.format(uid)
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
            #url='http://i.snssdk.com/dongtai/list/v9/?user_id={uid}&max_cursor={cursor}&callback=jsonp{num}'.format(uid=uid,cursor=max_cursor,num=json_num)
            url='http://i.snssdk.com/dongtai/list/v9/?user_id={uid}&max_cursor={cursor}'.format(uid=uid,cursor=max_cursor)
            res=requests.get(url,headers=heads,timeout=30)
            content = res.content.decode('utf-8')
            content = json.loads(content)
            if 'data' in content.keys() and 'data' in content['data'].keys():
                has_more = content['data']['has_more']
                max_cursor = content['data']['max_cursor']
                original_data=content['data']['data']
                if len(original_data) == 0:
                    rcli.sadd('err_err_set',uid)
                if len(original_data) != 0:
                    for x in original_data:
                        if re.match(r'^http.//toutiao.com/i.*$',x['share_url']) or re.match(r'^http.//weitoutiao.zjurl.cn/.*$',x['share_url']):
                        #if not re.match(r'^http.*//toutiao.com/dongtai.*$',x['share_url']):
                            try:
                                item_id = x['item_id_str'] if 'item_id_str' in x else x['id_str']
                            except KeyError:
                                continue
                            behot_time=x['create_time']
                            print('create_time:',time.strftime("%Y-%m-%d",time.localtime(behot_time)))
                            #is_exist=False if item_id in items_id else True
                            is_again_working =writer_fetch.check_time(behot_time,pool,uid)
                            #print('is_again_working',is_again_working)
                            if is_again_working:
                                put2es({'uid':uid,'original_data':x},pool,db)
                                con+=1
                            else:
                                has_more=False
                                break
                    print(uid,con,'page:',json_num)
                
                if not has_more:
                    print(uid + '_Dynamic overtime has stopped fetching')
                    return
            else:
                has_more =False
                
            json_num+=1
            time.sleep(1)
    except:       
        traceback.print_exc()
