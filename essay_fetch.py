#!/usr/bin/python3

import os
import random
import pickle
import requests
import json
from bs4 import BeautifulSoup
import time
import sys
import traceback
import threading
import redis
import re
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import writer_fetch


def to_es_body(info_gal,index_name,type_name):
    info_action = {}
    info_action['index_name']= index_name
    info_action['type_name']= type_name
    info_action['toutiaor_id']= info_gal['toutiaor_id']
    info_action['id']=info_gal['_id']
    info_action['title']= info_gal['title']
    info_action['content']= info_gal['content']
    info_action['images']=info_gal['images']
    info_action['labels']= info_gal['labels']
    info_action['read_count']=info_gal['read_count']
    info_action['comment_count']=info_gal['comments_count']
    info_action['up_count']= info_gal['up_count']
    info_action['down_count']=info_gal['down_count']
    info_action['published_at']= info_gal['published_at']
    info_action['crawled_at']=info_gal['crawled_at']
    info_action['type']= info_gal['type']
    info_action['duration']= info_gal['duration']
    info_action['avatar_img']=info_gal['avatar_img']
    info_action['name']=info_gal['name']
    info_action['introduction']=info_gal['introduction']
    info_action['follower_count']=info_gal['follower_count']
    info_action['fans_count']=info_gal['fans_count']
    return info_action


def fetchGallerys(es,db,gallery,user_agent):
    #conn = db.articles
    heads={}
    heads['User-Agent']=user_agent
    try:
        if gallery != None and gallery != {}:
            info_gal = {}
            item_id = gallery['item_id']
            uid = gallery['uid']
            source_url = 'http://www.toutiao.com/item/' + str(item_id)+'/#p=1'
            try:
                res = requests.get(source_url,headers=heads, timeout=15)
            except requests.exceptions.Timeout:
                res = requests.get(source_url, headers=heads, timeout=15)
            if re.match(r'^http://www.toutiao.com/i.*$', res.url) and res.status_code==200:
                bs = BeautifulSoup(res.content.decode('utf-8'), 'xml')
                #print(uid,item_id)
                script_len=len(bs.select('body script'))
                #print(script_len)
                if script_len<3:
                    #print(uid,item_id)
                    traceback.print_exc()
                user_info = bs.select('body script')[2].text.replace(' ', '').split(';\n')
                info=''
                for i in user_info:
                    if re.match(r'^var.*gallery.*$', i):
                        info =i
                        info.strip()
                #print(type(info))
                if re.match(r'^var.*gallery.*$',info):
                    infos = info.split('=')
                    info_str =('='.join(infos[1:]))
                    info_spl=info_str.strip().replace('\/', '/').replace('false', 'False').replace('true', 'True').replace('null', 'None')
                    #print(uid,item_id,info_spl)
                    gallery_info = eval(info_spl)
                    #print(gallery_info['labels'])
                    labels = gallery_info['labels']
                    info_gal['labels'] = labels
                    images = (x['url'] for x in gallery_info['sub_images'])
                    info_gal['images']=list(images)
                    info_gal['content'] = '\n'.join(gallery_info['sub_abstracts'])
                    info_gal['title']=gallery['title']
                    info_gal['duration']=gallery['duration']
                    info_gal['read_count']=gallery['read_count']
                    info_gal['comments_count']=gallery['comments_count']
                    info_gal['up_count']=gallery['up_count']
                    info_gal['down_count']=gallery['down_count']
                    info_gal['type']=gallery['type']
                    info_gal['published_at']=int(gallery['behot_time']*1000)
                    info_gal['toutiaor_id'] = uid
                    info_gal['_id'] = item_id
                    info_gal['crawled_at'] = int(time.time()*1000)
                    info_gal['avatar_img'] = gallery['avatar_img']
                    info_gal['name'] = gallery['name']
                    info_gal['introduction'] = gallery['introduction']
                    info_gal['follower_count'] = gallery['follower_count']
                    info_gal['fans_count'] = gallery['fans_count']
                    print(item_id,'This atlas has been resolved!')
                    return info_gal
    except:
        with open(os.path.abspath('.') + '/fetchGallerys_log.txt', 'a', encoding='utf-8',
                  errors='ignore') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) +'_'+gallery['item_id']+':\n')
            traceback.print_exc(file=f)
            f.write('------------------------------------------\n\n')
        traceback.print_exc()


def fetchArticles(es,db,article,user_agent):
    #conn = db.articles
    heads = {}
    heads['User-Agent'] = user_agent
    try:
        if article!=None and article!={}:
            info_gal = {}
            item_id = article['item_id']
            uid = article['uid']
            source_url = 'http://www.toutiao.com/item/' + str(item_id)+'/'
            try:
                res = requests.get(source_url,headers=heads,timeout=15)
            except requests.exceptions.Timeout:
                res = requests.get(source_url, headers=heads, timeout=15)
            if re.match(r'^http://www.toutiao.com/i.*$', res.url) and res.status_code==200:
                article_content=[]
                article_imgs = []
                labels=[]
                bs = BeautifulSoup(res.content.decode('utf-8'), 'xml')
                tags_p=(bs.select('div.article-content div p') if len(bs.select('div.article-content div p')) else bs.select('div.article-content p'))
                for p in tags_p:
                    imgs = p.select('img')
                    styles = p.select('style')
                    if len(imgs) == 0 and len(styles) == 0:
                        article_content.append(p.text)
                    elif len(styles) == 0:
                        for img in imgs:
                            article_imgs.append(img.get('src'))
                article_content='\n'.join(article_content)
                info_gal['content'] = article_content
                info_gal['images'] = article_imgs
                label_links=bs.select('ul.label-list li.label-item a.label-link')
                for link in label_links:
                    labels.append(link.text)
                info_gal['labels'] = labels
                info_gal['title'] = article['title']
                info_gal['duration'] = article['duration']
                info_gal['read_count'] = article['read_count']
                info_gal['comments_count'] = article['comments_count']
                info_gal['up_count'] = article['up_count']
                info_gal['down_count'] = article['down_count']
                info_gal['type'] = article['type']
                info_gal['published_at'] = int(article['behot_time']*1000)
                info_gal['toutiaor_id'] = uid
                info_gal['_id'] = item_id
                info_gal['crawled_at'] = int(time.time()*1000)
                info_gal['avatar_img'] = article['avatar_img']
                info_gal['name'] = article['name']
                info_gal['introduction'] = article['introduction']
                info_gal['follower_count'] = article['follower_count']
                info_gal['fans_count'] = article['fans_count']
                print(item_id, 'This article has been parsed')
                return info_gal
    except:
        traceback.print_exc()

def fetchVideos(es,db,video):
    #conn = db.articles
    try:
        if video!=None and video!={}:
            info_gal = {}
            item_id = video['item_id']
            uid = video['uid']
            info_gal['content'] = ''
            info_gal['images'] = ''
            info_gal['labels'] = []
            info_gal['title'] = video['title']
            info_gal['duration'] = video['duration']
            info_gal['read_count'] = video['read_count']
            info_gal['comments_count'] = video['comments_count']
            info_gal['up_count'] = video['up_count']
            info_gal['down_count'] = video['down_count']
            info_gal['type'] = video['type']
            info_gal['published_at'] = int(video['behot_time']*1000)
            info_gal['toutiaor_id'] = uid
            info_gal['_id'] = item_id
            info_gal['crawled_at'] = int(time.time()*1000)
            info_gal['avatar_img'] = video['avatar_img']
            info_gal['name'] = video['name']
            info_gal['introduction'] = video['introduction']
            info_gal['follower_count'] = video['follower_count']
            info_gal['fans_count'] = video['fans_count']
            print(item_id, 'This video has been resolved')
            return info_gal
    except:
        traceback.print_exc()

def fetchOthers(es,db,other,user_agent):
    #conn = db.articles
    heads = {}
    heads['User-Agent'] = user_agent
    try:
        if other!=None and other!={}:
            info_gal = {}
            item_id = other['item_id']
            uid = other['uid']
            source_url = 'http://www.toutiao.com/item/' + str(item_id)+'/'
            try:
                res = requests.get(source_url,headers=heads,timeout=15)
            except requests.exceptions.Timeout:
                res = requests.get(source_url, headers=heads, timeout=15)
            if re.match(r'^https://temai.snssdk.com/.*$', res.url) and res.status_code==200:
                other_content=[]
                other_imgs = []
                labels=[]
                bs = BeautifulSoup(res.content.decode('utf-8'), 'xml')
                contents = bs.select('figcaption')
                imgs = bs.select('img[alt-src]')
                for img in imgs:
                    img=img.get('alt-src')
                    other_imgs.append(img)
                for content in contents:
                    other_content.append(content.text)
                other_content='\n'.join(other_content)
                info_gal['content'] = other_content
                info_gal['images'] = other_imgs
                info_gal['labels'] = labels
                info_gal['title'] = other['title']
                info_gal['duration'] = other['duration']
                info_gal['read_count'] = other['read_count']
                info_gal['comments_count'] = other['comments_count']
                info_gal['up_count'] = other['up_count']
                info_gal['down_count'] = other['down_count']
                info_gal['type'] = 2
                info_gal['published_at'] = int(other['behot_time']*1000)
                info_gal['toutiaor_id'] = uid
                info_gal['_id'] = item_id
                info_gal['crawled_at'] = int(time.time()*1000)
                info_gal['avatar_img'] = other['avatar_img']
                info_gal['name'] = other['name']
                info_gal['introduction'] = other['introduction']
                info_gal['follower_count'] = other['follower_count']
                info_gal['fans_count'] = other['fans_count']
                print(item_id, 'The other atlas has been parsed')
                return info_gal
    except:
        traceback.print_exc()


def item_to_es(pool):
    rcli = redis.StrictRedis(connection_pool=pool)
    articles_to_es = []
    headers={'Content-Type':'application/json'}
    while True:
        try:
            if rcli.llen('item_ES_list')>=1000:
                while rcli.llen('item_ES_list')>100:
                    _item=rcli.rpop('item_ES_list')
                    if _item!=None:
                        item = eval(_item.decode())
                        articles_to_es.append(item)
                    if len(articles_to_es)>=500:
                        #helpers.bulk(es, articles_to_es, index='toutiao_articles_and_users', raise_on_error=True)
                        requests.post('http://59.110.52.213/stq/api/v1/pa/toutiao/add',headers=headers,data=json.dumps(articles_to_es))
                        print(str(len(articles_to_es))+'articles pushed into Elasticsearch')
                        del articles_to_es[0:len(articles_to_es)]
                if len(articles_to_es) >0:
                    #helpers.bulk(es, articles_to_es, index='toutiao_articles_and_users', raise_on_error=True)
                    requests.post('http://59.110.52.213/stq/api/v1/pa/toutiao/add', headers=headers,data=json.dumps(articles_to_es))
                    print(str(len(articles_to_es)) + 'articles pushed into Elasticsearch')
                    del articles_to_es[0:len(articles_to_es)]
        except:
            if len(articles_to_es) > 0:
                #helpers.bulk(es, articles_to_es, index='toutiao_articles_and_users', raise_on_error=True)
                requests.post('http://59.110.52.213/stq/api/v1/pa/toutiao/add', headers=headers,data=json.dumps(articles_to_es))
                print(str(len(articles_to_es)) + 'articles pushed into Elasticsearch')
                del articles_to_es[0:len(articles_to_es)]
            traceback.print_exc()
        time.sleep(3)

def toutiaor_join_article(item,db):
    try:
        conn = db.toutiaors
        uid = item['uid']
        if uid != None and uid != '':
            toutiaor=conn.find_one({'_id': uid})
            if toutiaor!=None and toutiaor!={}:
                del toutiaor['_id']
                del toutiaor['crawled_at']
                item.update(toutiaor)
                return item
            else:
                toutiaor=writer_fetch.fetchWriterTitle(uid=uid,user_agent='')
                del toutiaor['_id']
                del toutiaor['crawled_at']
                item.update(toutiaor)
                return item
    except:
        traceback.print_exc()


def fetch_working(pool,es,db1,db2,userAgents):
    agent_lens = len(userAgents)
    rcli = redis.StrictRedis(connection_pool=pool)
    db=''
    while True:
        try:
            if db1.client.is_primary :
                db=db1
                db2.client.close()
            elif db2.client.is_primary :
                db = db2
                db1.client.close()
            index = (random.randint(0, agent_lens - 1)) % agent_lens
            user_agent = userAgents[index]
            item=eval(rcli.brpop('item_AGV_list')[1].decode())
            item=toutiaor_join_article(item,db)
            work_type=(item['type'] if (item!=None and item!={}) else -1)
            if (work_type==0):
                item_article=fetchArticles(es,db,item,user_agent)
                if item_article!=None and item_article!={}:
                    item_article=to_es_body(item_article,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                    rcli.lpush('item_ES_list',item_article)
            elif (work_type==1):
                item_gallery = fetchGallerys(es,db, item,user_agent)
                if item_gallery!=None and item_gallery!={}:
                    item_gallery = to_es_body(item_gallery,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                    rcli.lpush('item_ES_list', item_gallery)
            elif (work_type==2):
                item_video = fetchVideos(es,db, item)
                if item_video != None and item_video != {}:
                    item_video = to_es_body(item_video,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                    rcli.lpush('item_ES_list', item_video)
            db.client.close()
            print('The resources is stored in the cache queue, waiting to be pushed into the Elasticsearch!')
        except:
            traceback.print_exc()

def fetch_essay(pool,es,db1,db2,userAgents):
    try:
        t1=threading.Thread(target=fetch_working,args=(pool,es,db1,db2,userAgents))
        t2 = threading.Thread(target=item_to_es, args=(pool,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except:
        traceback.print_exc()

