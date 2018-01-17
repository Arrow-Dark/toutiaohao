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
    # info_action = {}
    # info_action['index_name']= index_name
    # info_action['type_name']= type_name
    # info_action['toutiaor_id']= info_gal['toutiaor_id']
    # info_action['id']=info_gal['_id']
    # info_action['title']= info_gal['title']
    # info_action['content']= info_gal['content']
    # info_action['images']=info_gal['images']
    # info_action['labels']= info_gal['labels']
    # info_action['read_count']=info_gal['read_count']
    # info_action['comment_count']=info_gal['comments_count']
    # info_action['up_count']= info_gal['up_count']
    # info_action['down_count']=info_gal['down_count']
    # info_action['published_at']= info_gal['published_at']
    # info_action['crawled_at']=info_gal['crawled_at']
    # info_action['type']= info_gal['type']
    # info_action['duration']= info_gal['duration']
    # info_action['avatar_img']=info_gal['avatar_img']
    # info_action['name']=info_gal['name']
    # info_action['introduction']=info_gal['introduction']
    # info_action['follower_count']=info_gal['follower_count']
    # info_action['fans_count']=info_gal['fans_count']
    info_gal['is_finish']='true'
    return info_gal



def check_type(html):
    try:
        bs = BeautifulSoup(html, 'html.parser')
        scripts = bs.select('body script')
        if len(scripts):
            _len=list(x.text for x in scripts if re.match(r'^var BASE_DATA.*',x.text))
            if len(_len):
                user_info = _len[0].replace(' ', '').split('\n') if len(_len) else []
                if not len(user_info):
                    print('user_info is empty!')
                    return -1
            else:
                return -1
            for i in user_info:
                if re.match(r'^.*galleryInfo.*$', i.strip()):
                    return 1
                elif re.match(r'^.*articleInfo.*$', i.strip()):
                    return 0
        else:
            return -1
    except :
        traceback.print_exc()
        return -1

def rep_resouce(es,db,item,rcli,user_agent):
    heads={}
    heads['User-Agent']=user_agent
    try:
        if item != None and item != {}:
            item_id = item['id']
            uid = item['toutiaor_id']
            source_url = 'http://www.toutiao.com/i'+str(item_id)+'/'
            try:
                res = requests.get(source_url,headers=heads, timeout=15)
            except requests.exceptions.Timeout:
                res = requests.get(source_url, headers=heads, timeout=15)
            html=res.content.decode('utf-8')
            if re.match(r'^.*://.*toutiao.com/.*$', res.url) and res.status_code==200:
                _type=check_type(html)
                item['type']=_type
                print('item_type',_type)
                if _type==0:
                    try:
                        item_article=fetchArticles(es,db,item,html)
                    except:
                        traceback.print_exc()
                        rcli.lpush('item_AGV_list',item)
                        item_article=None
                    if item_article!=None:
                    #if item_article!=None:
                        item_article=to_es_body(item_article,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                        rcli.lpush('item_ES_list',item_article)
                        print('into es_list')
                elif _type==1:
                    try:
                        item_gallery = fetchGallerys(es,db, item,html)
                    except:
                        traceback.print_exc()
                        item['type']=0
                        rcli.lpush('item_AGV_list',item)
                        item_gallery=None
                    if item_gallery!=None and len(item_gallery['images']):
                        item_gallery = to_es_body(item_gallery,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                        rcli.lpush('item_ES_list', item_gallery)
                else:
                    item['type']=0
                    rcli.lpush('item_AGV_list',item)
            elif re.match(r'^http.://temai.snssdk.com/.*$', res.url):
                try:
                    item_other = fetchOthers(es,db,item,html)
                except:
                    item['type']=0
                    rcli.lpush('item_AGV_list',item)
                    item_other=None
                if item_other != None:
                    item_other = to_es_body(item_other,index_name='toutiao_articles_and_users',type_name='toutiao_articles_and_users')
                    rcli.lpush('item_ES_list', item_other)
            else:
                item['flag']+=1
                rcli.lpush('item_AGV_list',item)
    except:
        item['flag']+=1
        rcli.lpush('item_AGV_list',item)
        #db.err_item.update({'_id':item_id},item,True)
        traceback.print_exc()

def fetchGallerys(es,db,gallery,html):
    if gallery != None and gallery != {}:
        #info_gal = {}
        # item_id = gallery['id']
        # uid = gallery['toutiaor_id']
        bs = BeautifulSoup(html, 'html.parser')
        #print(uid,item_id)
        scripts=bs.select('body script')
        #print(script_len)
        _len=list(x.text for x in scripts if re.match(r'^var BASE_DATA.*',x.text))
        user_info = _len[0].replace(' ', '').split('\n')
        info=''
        for i in user_info:
            if re.match(r'^.*gallery:.*JSON.parse.*$', i):
                info = i.strip()
                break
        #print(type(info))
        if info!='':
            info_json=info[info.index('{'):info.rindex('}')+1].replace(r'\"','"').replace(r'\\','\\')
            #print(uid,item_id,info_spl)
            gallery_info = json.loads(str(info_json))
            #print(gallery_info['labels'])
            labels = gallery_info['labels']
            gallery['labels'] = labels
            images = (x['url'] for x in gallery_info['sub_images'])
            gallery['images']=list(images)
            gallery['content'] = '\n'.join(gallery_info['sub_abstracts'])
            gallery['crawled_at'] = int(time.time()*1000)
            print(gallery['id'],'This gallery has been resolved!')
        else:
            gallery['crawled_at'] = int(time.time()*1000)
            print(gallery['id'],'This gallery is empty!')
        return gallery


def fetchArticles(es,db,article,html):
    article_content=[]
    article_imgs = []
    labels=[]
    bs = BeautifulSoup(html, 'html.parser')
    tags_p=(bs.select('div.article-content div p') if len(bs.select('div.article-content div p')) else bs.select('div.article-content p'))
    #print(tags_p)
    if len(tags_p):
        for p in tags_p:
            imgs = p.select('img')
            styles = p.select('style')
            if len(imgs) == 0 and len(styles) == 0:
                article_content.append(p.text)
            elif len(styles) == 0:
                for img in imgs:
                    article_imgs.append(img.get('src'))
        labels=list(link.text for link in bs.select('ul.label-list li.label-item a.label-link'))
    else:
        scris=bs.select('body script')
        scri=list(x.text for x in scris if re.match(r'^var BASE_DATA.*',x.text))
        _text=scri[0][scri[0].index('{'):-1] if len(scri) else ''
        con=list(x for x in _text.split('\n') if re.match(r'^.*content:.*',x))
        _str=con[0].replace(' ','').replace('&lt;p','').replace('&lt','').replace(';/p','').replace('&quot','').replace('&gt','').replace('imgsrc&#x3D','')
        article_content=list(x for x in _str.split(';') if re.match('^[\u4e00-\u9fa5]',x)) if not re.match(r'^1\d+$',article['id']) else list(x for x in _str.split(';'))
        article_imgs=list(x for x in _str.split(';') if re.match('^http.*',x))
        tags=list(x for x in _text.split('\n') if re.match(r'^.*tags:.*',x))
        _str=con[0].replace(' ','').replace('&lt;p','').replace('&lt','').replace(';/p','').replace('&quot','').replace('&gt','').replace('imgsrc&#x3D','') if len(con) else ''
        tags=tags[0].replace(' ','') if len(tags) else None
        tag_list=eval(tags[tags.index('['):-1]) if tags else []
        labels=list(y[0] for y in (list(x.values()) for x in tag_list))

    article_content='\n'.join(article_content)
    article['content'] = article_content
    article['images'] = article_imgs

    article['labels'] = labels
    article['crawled_at'] = int(time.time()*1000)
    print(article['id'], 'This article has been parsed')
    return article

def fetchOthers(es,db,other,html):
    other_content=[]
    other_imgs = []
    labels=[]
    bs = BeautifulSoup(html, 'html.parser')
    contents = bs.select('figcaption')
    imgs = bs.select('img[alt-src]')
    for img in imgs:
        img=img.get('alt-src')
        other_imgs.append(img)
    for content in contents:
        other_content.append(content.text)
    other_content='\n'.join(other_content)
    other['content'] = other_content
    other['images'] = other_imgs
    other['labels'] = labels
    info_gal['crawled_at'] = int(time.time()*1000)
    print(item_id, 'The other atlas has been parsed')
    return info_gal


def item_to_es(pool):
    rcli = redis.StrictRedis(connection_pool=pool)
    articles_to_es = []
    headers={'Content-Type':'application/json'}
    while True:
        try:
            if rcli.llen('item_ES_list')>=1000:
                while rcli.llen('item_ES_list')>0:
                    _item=rcli.rpop('item_ES_list')
                    if _item!=None:
                        item = eval(_item.decode())
                        articles_to_es.append(item)
                    if len(articles_to_es)>=0:
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
        time.sleep(10)

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
    print('fetch_working')
    rcli = redis.StrictRedis(connection_pool=pool)
    #rcli=redis.StrictRedis()
    db=''
    while True:
        try:
            if db1.client.is_primary :
                db=db1
                db2.client.close()
            elif db2.client.is_primary :
                db = db2
                db1.client.close()
            user_agent = random.choice(userAgents)
            
            item=eval(rcli.brpop('item_AGV_list')[1].decode())
            #print(item.keys())
            flag=item['flag'] if item and 'flag' in item.keys() else 0
            if flag>5:
                db.err_item.update({'_id':item_id},item,True)
                print('err_item goto mongo!')
                continue
            else:
                item['flag']=flag
            work_type=item['type'] if (item!=None and item!={}) else -1
            print('work_type',work_type)
            
            if work_type==0:
                rep_resouce(es,db,item,rcli,user_agent)
                print('The resources is stored in the cache queue, waiting to be pushed into the Elasticsearch!')
            db.client.close()
            
        except:
            traceback.print_exc()
        time.sleep(5)

def fetch_essay(pool,es,db1,db2,userAgents):
    try:
        t1=threading.Thread(target=fetch_working,args=(pool,es,db1,db2,userAgents))
        t3=threading.Thread(target=fetch_working,args=(pool,es,db1,db2,userAgents))
        t4=threading.Thread(target=fetch_working,args=(pool,es,db1,db2,userAgents))
        t2 = threading.Thread(target=item_to_es, args=(pool,))
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
    except:
        traceback.print_exc()

