import os
import requests
import json
import time
import traceback
import homePage_fetch
import redis
import threading


def fetchSorts(pool,lock):
	rcli = redis.StrictRedis(connection_pool=pool)
	head={}
	max_behot_time=0
	count_=0
	boolen='true'
	if rcli.llen('sortsQueue')==0:
		homePage_fetch.distil()
	if rcli.llen('sortsQueue')>0:
		global href
		href=str(rcli.rpop('sortsQueue'))
	while True:
		try:
			url='http://www.toutiao.com/api/pc/feed/?category='+href.split('/')[1]+'&utm_source=toutiao&widen=1&max_behot_time='+str(max_behot_time)+'&max_behot_time_tmp='+str(max_behot_time)+'&tadrequire='+boolen
			res=requests.get(url,headers=head,timeout=30)
			#r=requests.get('http://jsonip.com',timeout=30)
			#proIp=json.loads(r.text)['ip']
			#print('当前使用ip'+ip)
			if count_==0:
				cook=res.headers['Set-Cookie'][:106]
				head['Cookie']=cook
			#json_dict=json.loads(res.content.decode())
			json_dict=json.loads(res.text)

			datas=json_dict['data']
			if len(datas)>0 and count_<100:
				for data in datas:
					if data['tag']!='ad' and data['tag'].strip()!='' and ('media_url' in data):
						if len(data['media_url'].split('/'))>=4:
							uid=data['media_url'].split('/')[3]
							if lock.acquire():
								if (not rcli.sismember('unWordUid_set',uid)):
									rcli.sadd('unWordUid_set',uid)
									rcli.lpush('unWordUid_list',uid)
									print(uid,':from '+href.split('/')[1]+'未重')
								lock.release()
								print(uid, ':from ' + href.split('/')[1] + '重复')


				max_behot_time=json_dict['next']['max_behot_time']
				count_+=1
				if boolen=='true':
					boolen='false'
				else:
					boolen='true'
				#print(count_)
				#print(max_behot_time)
				time.sleep(3)
			else:
				break
		except:
			if not os.path.exists(os.path.abspath('.')+'/errLogs'):
				os.mkdir(os.path.abspath('.')+'/errLogs')
			with open(os.path.abspath('.')+'/errLogs'+'/fetchSorts_log_new.txt','a',encoding='utf-8',errors='ignore') as f:
				f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+':\n')
				traceback.print_exc(file=f)
				f.write('------------------------------------------\n\n')
				traceback.print_exc()

class fetchSortsThread(threading.Thread):
	def __init__(self,pool,lock):
		threading.Thread.__init__(self)
		self.pool=pool
		self.lock=lock
	def run(self):
		fetchSorts(self.pool, self.lock)

if __name__=='__main__':
	while True:
		fetchSorts()
