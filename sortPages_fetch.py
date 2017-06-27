from selenium import webdriver
import homePage_fetch
import os
import sys
import traceback
import time
import shutil
import re
from bs4 import BeautifulSoup
import redis

def scroll(n,i):
	return 'window.scrollTo(0,(document.body.scrollHeight/{0})*{1});'.format(n,i)


rcli=redis.StrictRedis(host='192.168.80.132',port=6379,db=0)
def fetchSorts():
	urls=homePage_fetch.distil()
	'''
	browser_pro=webdriver.FirefoxProfile()
	#browser_pro.set_preference('permissions.default.stylesheet',2)

	browser_pro.set_preference('permissions.default.image',2)
	browser_pro.set_preference('javascript.enable',False)
	browser_pro.update_preferences()
	#browser=webdriver.PhantomJS()
	browser=webdriver.Firefox(browser_pro)
	'''
	browser=webdriver.PhantomJS()
	browser.maximize_window()
	length=len(urls)
	for i in range(length):
		try:
			url='http://www.toutiao.com/'+urls.pop()
			#href=url.split(' ')[0]
			#name=url.split(' ')[1]
			browser.get(url)
			#time.sleep(10)
			n=10
			for i in range(1,501):
				s=scroll(n,i)
				print(i)
				browser.execute_script(s)
				time.sleep(1)
				'''
			with open(os.path.abspath('.')+'/sortsPage'+'/'+name+'.html','w',encoding='utf-8',errors='ignore') as f:
				f.write(browser.page_source)
				'''
			bs=BeautifulSoup(browser.page_source,'lxml')
			lis=bs.select('.wcommonFeed ul li') #.find_all(attrs={"ga_event":'video_item_click','article_item_click','gallery_item_click'})
			#print(lis)
			for li in lis:
				if li.get('ga_event')!='ad_item_click' and li.get('ga_event')!='refresh_item_click':
					a=li.select('a[class="lbtn source"]')[0]
					ahref=a.get('href')					
					if re.match('^.*/c/user/.*$', ahref):
						#print(ahref)
						uid=ahref.split('/')[3]
						if (not rcli.sismember('wordedUid',uid)) and (not rcli.sismember('unWordUid_set',uid)):
							rcli.sadd('unWordUid_set',uid)
							rcli.lpush('unWordUid_list',uid)
							print(uid)

		except:
			traceback.print_exc(file=f)
			if not os.path.exists(os.path.abspath('.')+'/errLogs'):
				os.mkdir(os.path.abspath('.')+'/errLogs')
			with open(os.path.abspath('.')+'/errLogs'+'/fetchSorts_log.txt','a',encoding='utf-8',errors='ignore') as f:
				f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+':\n')				
				f.write('------------------------------------------\n\n')
	browser.quit()
		
	
def delSortPages():
	try:
		if not os.path.exists(os.path.abspath('.')+'/sortsPage'):
			os.mkdir(os.path.abspath('.')+'/sortsPage')
		else:
			shutil.rmtree(os.path.abspath('.')+'/sortsPage')
			os.mkdir(os.path.abspath('.')+'/sortsPage')
	except:
		traceback.print_exc()
	finally:
		shutil.rmtree(os.path.abspath('.')+'/sortsPage')
		os.mkdir(os.path.abspath('.')+'/sortsPage')

if __name__=='__main__':
	#delSortPages()
	try:
		while True:
			fetchSorts()
	except:
		sys.exit(1)	
