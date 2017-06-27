import os
import random
import pickle
import requests
import json
from bs4 import BeautifulSoup
import shutil
import time
import sys
import traceback
import threading
import redis


def readWords(rcli):
	#rcli=redis.StrictRedis(connection_pool=pool)
	with open(os.path.abspath('.')+'/keyWords/words.txt','r',-1,encoding='utf-8',errors='ignore') as f:
		words=f.read().split(' ')	
		for word in words:
			rcli.lpush('keyWords_list',word)

def getKeyWord(rcli):
	#rcli=redis.StrictRedis(connection_pool=pool)
	try:
		if rcli.llen('keyWords_list')==0:
			readWords(rcli)
		word=rcli.rpop('keyWords_list').decode('utf-8')
		return word
	except:
		traceback.print_exc()


def ckeck_id_key(data):
	if 'user_id' in data.keys() and data['user_id'] != '' and data['user_id'] != None:
		id_key = 'user_id'
		return id_key
	elif 'id' in data.keys() and data['id'] != '' and data['id'] != None:
		id_key = 'id'
		return id_key
	else:
		print('id_key_Error!')
		raise KeyError



def lock_and_intoRedis(uid,rcli):
	if (not rcli.sismember('unWordUid_set', uid)):
		with rcli.pipeline() as pipe:
			pipe.multi()
			pipe.sadd('unWordUid_set', uid)
			pipe.rpush('unWordUid_list', uid)
			pipe.execute()
		print(uid+':inside_Queue!')


def cur_tab1_url_open(rcli,word):
	#rcli = redis.StrictRedis(connection_pool=pool)
	#word=getKeyWord(rcli)
	#print(word+str(num)+'\n')
	offset=0
	while True:
		try:
			url='http://www.toutiao.com/search_content/?offset='+str(offset)+'&format=json&keyword='+word+'&autoload=true&count=20&cur_tab=1'
			#break
			try:
				res = requests.get(url, timeout=30)
			except requests.exceptions.Timeout:
				res = requests.get(url, timeout=30)
			json_dict=json.loads(res.text)
			datas=json_dict['data']
			has_more=json_dict['has_more']
			id_key = 'media_creator_id'
			if has_more!=1 and len(datas)==0:
				break
			elif has_more!=1 and len(datas)!=0:
				for data in datas:
					if id_key in data.keys():
						uid=str(data[id_key])
						lock_and_intoRedis(uid, rcli)
				break
			elif has_more==1 and  len(datas)!=0:
				for data in datas:
					if id_key in data.keys():
						uid=str(data[id_key])
						lock_and_intoRedis(uid, rcli)
				offset=offset+20
		except:
			with open(os.path.abspath('.')+'/errLogs'+'/searchKeyWord_log.txt','a',encoding='utf-8',errors='ignore') as f:
				f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'_word='+word+':\n')
				traceback.print_exc(file=f)
				f.write('------------------------------------------\n\n')
				traceback.print_exc()
			break



def cur_tab4_url_open(rcli,word):
	#rcli = redis.StrictRedis(connection_pool=pool)
	#word=getKeyWord(rcli)
	#print(word+str(num)+'\n')
	offset=0
	while True:
		try:
			url='http://www.toutiao.com/search_content/?offset='+str(offset)+'&format=json&keyword='+word+'&autoload=true&count=20&cur_tab=4'
			#break
			try:
				res = requests.get(url, timeout=30)
			except requests.exceptions.Timeout:
				res = requests.get(url, timeout=30)
			json_dict=json.loads(res.text)
			datas=json_dict['data']
			has_more=json_dict['has_more']
			if has_more!=1 and len(datas)==0:
				break
			elif has_more!=1 and len(datas)!=0:
				for data in datas:
					id_key=ckeck_id_key(data)
					uid=str(data[id_key])
					lock_and_intoRedis(uid, rcli)
				break
			elif has_more==1 and  len(datas)!=0:
				for data in datas:
					id_key = ckeck_id_key(data)
					uid=str(data[id_key])
					lock_and_intoRedis(uid, rcli)
				offset=offset+20
		except:
			with open(os.path.abspath('.')+'/errLogs'+'/searchKeyWord_log.txt','a',encoding='utf-8',errors='ignore') as f:
				f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'_word='+word+':\n')
				traceback.print_exc(file=f)
				f.write('------------------------------------------\n\n')
				traceback.print_exc()
			break

def cur_tab_url_open(pool):
	rcli = redis.StrictRedis(connection_pool=pool)
	word=getKeyWord(rcli)
	cur_tab1_url_open(rcli, word)
	cur_tab4_url_open(rcli, word)