from selenium import webdriver
from bs4 import BeautifulSoup
from UtilsPy import toutiaoUtils
import re
import time
import redis
url=toutiaoUtils.homePageUrls()
sortsQueue=toutiaoUtils.sortsQueue()
#workedQueue={}
wordAbandoned=toutiaoUtils.wordAbandoned()

def distil():
	browser=webdriver.PhantomJS()
	rcli = redis.StrictRedis(host='101.201.37.28', port=6379,password='r-2zee00173ec68024.redis.rds.aliyuncs.com:Abc123456')
	#browser=webdriver.Firefox()
	browser.get(url)
	html=browser.page_source
	browser.quit()
	bs=BeautifulSoup(html,'lxml')
	sorts=bs.select('.wchannel-item')
	for a in sorts:
		name=a.select('span')[0].string
		if name not in wordAbandoned:
			href=a.get('href')
			if (re.match('^.*/news_.*$', href) or re.match('^.*/funny.*$', href)) and url not in sortsQueue:
				#sortsQueue.append(href)
				#rcli.sadd('unWordUid_set',uid)
				rcli.lpush('sortsQueue',href)

def popHref(sortsQueue):
	if len(sortsQueue)==0:
		sortsQueue=distil()
	if len(sortsQueue)!=0:
		return sortsQueue.pop()

if __name__=='__main__':
	print(distil())
	
