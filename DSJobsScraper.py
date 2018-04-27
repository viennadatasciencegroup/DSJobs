#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
o BeautifulSoup: https://www.analyticsvidhya.com/blog/2015/10/beginner-guide-web-scraping-beautiful-soup-python/

Open SQL DB
Read Jobs from Job DB
Write Jobs to DB

Use Decision tree model to analyze Jobs

2DO
o Schedule day run
o Use IP Switcher Stem: https://dm295.blogspot.co.at/2016/02/tor-ip-changing-and-web-scraping.html?m=1
o NLP Textblob: http://textblob.readthedocs.io/en/dev/quickstart.html | https://www.analyticsvidhya.com/blog/2018/02/natural-language-processing-for-beginners-using-textblob/

'''

import requests
import re
from bs4 import BeautifulSoup
from DSJobsDB import Job
import time
import datetime
import warnings
#import cgi
#import cgitb; cgitb.enable()  # for troubleshooting
from stem import Signal
from stem.control import Controller

def get_tor_session()
    '''
    This function changes the IP address of your search request. It uses the TOR network.
    Please start the TOR service "service tor start" on your server to make this work.
    Find the setup instructions here:
    https://stackoverflow.com/questions/30286293/make-requests-using-python-over-tor
    '''
    renew_connection()

    session = requests.session()
    session.proxies = {'http': 'socks5://127.0.0.1:9050',
                       'https': 'socks5://127.0.0.1:9050'}

#   print(session.get("http://httpbin.org/ip").text)

    return session

def renew_connection():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password='my password')
        controller.signal(Signal.NEWNYM)

def GetWebPage(url):
    '''
    Query the html page and return a beautiful soup object.
    '''
    
    #query the website
    #page = requests.get(url)
    session = get_tor_session()
    page = session.get(url)

    #create nice html file
    soup=BeautifulSoup(page.content)#,"lxml")
    return soup

def GetKarriereAtJobDetail(url_):
    '''
    follow the link to the detailed job description and return the information.
    '''
    
    page = requests.get(url_)
    
    x0_ = str(page.content)
    x1_ =  x0_.find("application/ld+json")
    x2_ =  x0_[x1_+20:].find("</script")
    
    return x0_[x1_+21:x1_+x2_+20]

def GetKarriereAtJob(job,src):
    '''
    use keyphrases to generate a URL. Walk though the list of jobs and return their 
    url, description, id and detailed job desciption.
    If the last job on the page was found, use the same keyword, but increase the
    pagecount by 1. if the first job on the new page is different from the first job
    of the last page, get all jobs from the page. Otherwise break the loop.
    '''
    
    searchString_ = 'https://www.karriere.at/jobs?keywords='
        
    PrintToHtml('Attempt to read: ' + searchString_ + job)
    
    a=1
    webData = GetWebPage(searchString_ + job + '&page=' + str(a))
        
    #PrintToHtml(str(webData))
    
    src_=[]
    url_=[]
    job_=[]
    id_=[]
    when_=[]
    company_=[]
    jobDetail_=[]
    
    #Browse all pages on Karriere.at containing the search string, until the last page was found
    jobFirstID_ = ''
    jobExit_ = 0
    
    while (jobExit_ == 0):
        
        PrintToHtml('Read jobs: '+str(job) +', page: '+str(a))
        
        jobFirst_ = 0
        
        for div in webData.find_all('div', attrs={'class':'m-jobItem__dataContainer'}):
            src_.append(src)
            url_.append(div.find('a')['href'])
            job_.append(div.find('a').contents[0])
            id_.append(div.find('a')['href'][-7:])
            
            #Get the Job Details from new URSL
            jobDetail_.append(GetKarriereAtJobDetail(div.find('a')['href']))
            
            if (jobFirst_ == 0):
                JobAgain_ = str(div.find('a')['href'][-7:])
                if (str(JobAgain_) == str(jobFirstID_)):
                    jobExit_ = 1
                else:
                    jobFirstID_ = div.find('a')['href'][-7:]
            
            jobFirst_ += 1
    
        for div in webData.find_all(class_=re.compile("m-jobItem__date")):
            when_.append(div.contents[0])
            
        for div in webData.find_all(class_=re.compile("m-jobItem__company")):
            company_.append(div.contents[0])
   
        a += 1
        
        if a > 25:
            jobExit_ = 1
            PrintToHtml('Search String '+searchString_ + job + '&page=' + str(a)+' provides no result')
        
        webData = GetWebPage(searchString_ + job + '&page=' + str(a))
    
    #Write all elemts of the List items to the database
    #with Job() as db:
    #db = Job()
    with Job() as db:
        a = 0
        while a < len(id_):
            #print('Save Job: '+str(a)+' ID: '+id_[a])
            db.writeJob(src_[a], job, id_[a], url_[a],job_[a],when_[a],company_[a])
            db.writeJobDetail(src_[a],id_[a],jobDetail_[a],'')#, JobDetailClean(jobDetail_[a]))
            a += 1
            
    PrintToHtml(str(a) + ' Jobs updated sucessfully')
    
def PrintToHtml(message):

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    with open('debug.html', 'a') as myfile:
        myfile.write('<p>' +str(st) + " | " + message + '</p>')


    #myfile.close()
    
def UpdateKarriereAt():
    '''
    Select keyphrases to put into the searchengine "www.karriere.at" 
    '''
    
    PrintToHtml("Start UpdateKarriereAt")
      
    GetKarriereAtJob('data+science','www.karriere.at')
    GetKarriereAtJob('data+scientist','www.karriere.at')
    GetKarriereAtJob('big+data','www.karriere.at')
    GetKarriereAtJob('hadoop','www.karriere.at')
    GetKarriereAtJob('machine+learning','www.karriere.at')
    GetKarriereAtJob('artificial+intelligence','www.karriere.at')
    GetKarriereAtJob('predictive+analytics','www.karriere.at')
    GetKarriereAtJob('scala','www.karriere.at')
    GetKarriereAtJob('hive','www.karriere.at')
    GetKarriereAtJob('tensorflow','www.karriere.at')
    GetKarriereAtJob('watson','www.karriere.at')
    GetKarriereAtJob('azure+ml','www.karriere.at')
    GetKarriereAtJob('spark','www.karriere.at')
    GetKarriereAtJob('data+mining','www.karriere.at')
    GetKarriereAtJob('decision+tree','www.karriere.at')
    GetKarriereAtJob('deep+learning','www.karriere.at')
    
    PrintToHtml("End UpdateKarriereAt")
    
    #5089767

def main():
    '''
    update the database
    '''
    warnings.simplefilter("ignore", DeprecationWarning)
    
    with open('debug.html','w') as myfile:
        myfile.write("""<html>
        <head></head>
        <body><p></p>{htmlText}</body>
        </html>""")
        
    #print('Hello World')
    UpdateKarriereAt()
    
if __name__ == "__main__":
    main()
