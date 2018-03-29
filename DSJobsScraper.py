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
import pymysql
from time import gmtime, strftime
#from collections import Counter

class Job(object):
    
    def __init__(self):
        self.conn = pymysql.connect(host="172.17.0.2", user="root", passwd="root", db="DSJobs", use_unicode=True, charset="utf8") 
        self.cur = self.conn.cursor()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            
    def writeJob(self, src_, job, id_, url_,job_,when_,company_):
        #ALTER TABLE JobListings CONVERT TO CHARACTER SET utf8
        qDate_ = strftime("%Y/%m/%d", gmtime())
        job_ = job_.replace("'", "")
        self.cur.execute("INSERT INTO JobListings (JobSource, QueryDate, SearchKey, JobSourceID, JobUrl, JobName, JobListingDate, Company) VALUES ('"+src_+"','"+qDate_+"','"+job+"',"+id_+",'"+url_+"','"+job_+"','"+when_+"','"+company_+"');")#'"+short_+"'
        self.conn.commit()
        
    def writeJobDetail(self, src_,id_, jobDetail_, jobDetailClean_):
        #ALTER TABLE JobListings CONVERT TO CHARACTER SET utf8
        jobDetail_ = jobDetail_.replace("'", "")
        
        self.cur.execute("INSERT IGNORE INTO JobDetail (JobSource, JobSourceID, JobDetail) VALUES ('"+src_+"',"+id_+",'"+jobDetail_+"');")#'"+short_+"'
        self.conn.commit()
        
    def readJobDetail(self,id_):
        self.cur.execute("SELECT JobDetail FROM JobDetail WHERE JobSourceID = " + str(id_))
        return(self.cur.fetchall())
        
def GetWebPage(url):
    '''
    Query the html page and return a beautiful soup object.
    '''
    
    #query the website
    page = requests.get(url)

    #create nice html file
    soup=BeautifulSoup(page.content,"lxml")
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
    
    a=1
    webData = GetWebPage(searchString_ + job + '&page=' + str(a))
    
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
        
        print('Parse job: '+str(job) +', page: '+str(a))
        
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
        
        webData = GetWebPage(searchString_ + job + '&page=' + str(a))
    
    #Write all elemts of the List items to the database
    with Job() as db:
        a = 0
        while a < len(id_):
            #print('Save Job: '+str(a)+' ID: '+id_[a])
            db.writeJob(src_[a], job, id_[a], url_[a],job_[a],when_[a],company_[a])
            db.writeJobDetail(src_[a],id_[a],jobDetail_[a],'')#, JobDetailClean(jobDetail_[a]))
            a += 1
            
    print(str(a) + ' Jobs updated sucessfully')
    
def UpdateKarriereAt():
    '''
    Select keyphrases to put into the searchengine "www.karriere.at" 
    '''
    
    print("Start UpdateKarriereAt")
      
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
    
    print("End UpdateKarriereAt")

def main():
    '''
    update the database
    '''
    
    UpdateKarriereAt()
    
if __name__ == "__main__":
    main()
