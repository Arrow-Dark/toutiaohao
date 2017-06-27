#/usr/bin/python3

import os
import time
import sys
import traceback
import threading
import redis

def perkArticles(uid,articles,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        if len(articles):
            for article in articles:
                if len(article.keys()):
                    item = {
                        'uid': uid,
                        'item_id': article['item_id'],
                        'title': article['title'],
                        'duration': 0,
                        'read_count': article['go_detail_count'],
                        'comments_count': article['comments_count'],
                        'up_count': article['like_count'],
                        'down_count':0,
                        'behot_time': article['behot_time']
                    }
                    item['type']=0
                    rcli.lpush('item_AGV_list',item)
    except:
        traceback.print_exc()

def perkGallerys(uid,galleries,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        if len(galleries):
            for gallery in galleries:
                if len(gallery.keys()):
                    go_detail_count=0
                    like_count=0
                    if 'go_detail_count' in gallery.keys():
                        go_detail_count=gallery['go_detail_count']
                    if 'like_count' in gallery.keys():
                        go_detail_count=gallery['like_count']
                    item = {
                        'uid': uid,
                        'item_id': gallery['item_id'],
                        'title': gallery['title'],
                        'duration': 0,
                        'read_count': go_detail_count,
                        'comments_count': gallery['comments_count'],
                        'up_count': like_count,
                        'down_count':0,
                        'behot_time': gallery['behot_time']
                    }
                    item['type'] = 1
                    rcli.lpush('item_AGV_list', item)
    except:
        traceback.print_exc()

def perkVideos(uid,videos,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        if len(videos):
            for video in videos:
                if len(video.keys()):
                    item = {
                        'uid': uid,
                        'item_id': video['item_id'],
                        'title': video['title'],
                        'duration': video['video_duration_str'],
                        'read_count': video['detail_play_effective_count'],
                        'comments_count': video['comments_count'],
                        'up_count': video['digg_count'],
                        'down_count': video['bury_count'],
                        'behot_time': video['behot_time']
                    }
                    item['type'] = 2
                    rcli.lpush('item_AGV_list', item)
    except:
        traceback.print_exc()

def perkOthers(uid,others,pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        if len(others):
            for other in others:
                if len(other.keys()):
                    item = {
                        'uid': uid,
                        'item_id': other['item_id'],
                        'title': other['title'],
                        'duration': 0,
                        'read_count': other['go_detail_count'],
                        'comments_count': other['comments_count'],
                        'up_count': other['like_count'],
                        'down_count': 0,
                        'behot_time': other['behot_time']
                    }
                    item['type'] = 3
                    rcli.lpush('item_AGV_list', item)
    except:
        traceback.print_exc()

class perkArticlesThread(threading.Thread):
    def __init__(self,uid,articles,pool):
        threading.Thread.__init__(self)
        self.uid = uid
        self.articles=articles
        self.pool=pool
    def run(self):
        perkArticles(self.uid,self.articles,self.pool)

class perkGalleriesThread(threading.Thread):
    def __init__(self,uid,galleries,pool):
        threading.Thread.__init__(self)
        self.uid = uid
        self.galleries=galleries
        self.pool=pool
    def run(self):
        perkGallerys(self.uid,self.galleries,self.pool)

class perkVideosThread(threading.Thread):
    def __init__(self,uid,videos,pool):
        threading.Thread.__init__(self)
        self.uid = uid
        self.videos=videos
        self.pool=pool
    def run(self):
        perkVideos(self.uid,self.videos,self.pool)

class perkOthersThread(threading.Thread):
    def __init__(self,uid,others,pool):
        threading.Thread.__init__(self)
        self.uid = uid
        self.others=others
        self.pool=pool
    def run(self):
        perkOthers(self.uid,self.others,self.pool)


def perk_item(listOfWorks,pool):
    uid=listOfWorks['_id']
    articles=listOfWorks['articles']
    galleries = listOfWorks['galleries']
    videos = listOfWorks['videos']
    others=listOfWorks['others']
    t_articles=perkArticlesThread(uid,articles,pool)
    t_galleries = perkGalleriesThread(uid, galleries, pool)
    t_videos = perkVideosThread(uid, videos, pool)
    t_others=perkOthersThread(uid, others, pool)
    t_articles.start()
    t_galleries.start()
    t_videos.start()
    t_others.start()
    t_articles.join()
    t_galleries.join()
    t_videos.join()
    t_others.join()
    print('The list of articles is sorted, and the queue of articles waits to be resolved!')