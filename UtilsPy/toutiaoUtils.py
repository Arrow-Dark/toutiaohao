#toutiaoUtils
from selenium import webdriver
import os
def homePageUrls():
	with open(os.path.abspath('.')+'/homePageUrl.txt','r',-1,encoding='utf-8',errors='ignore') as f:
		url=f.read()
		f.close()
		return url

def wordAbandoned():
	with open(os.path.abspath('.')+'/wordAbandoned.txt','r',-1,encoding='utf-8',errors='ignore') as f:
		word=list(f.read())
		f.close()
		#print(word)
		return word
		
def sortsQueue():
	myQueue=[]
	return myQueue

def getBrowser():
	browser=webdriver.PhantomJS()
	return browser
	
	
if __name__=='__main__':
	wordAbandoned()