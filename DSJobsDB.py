#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 30 15:50:09 2018

@author: user
"""

import pymysql
from time import gmtime, strftime
import time
import datetime
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
        
        self.PrintToHtml('Save jobdetail to DB: '+str(id_))
        
    def readJobDetail(self,index_):
        self.cur.execute("SELECT JobSourceID, JobDetail FROM JobDetail ORDER BY JobSourceID LIMIT " + str(index_) + ", 1;")
        return(self.cur.fetchall())
        
    def writeJobDetailClean(self, id_, jobDetailClean_):
        #ALTER TABLE JobListings CONVERT TO CHARACTER SET utf8
        jobDetailClean_ = jobDetailClean_.replace("'", "")
        
        self.cur.execute("UPDATE JobDetail SET JobDetailClean = '" + jobDetailClean_ + "' WHERE JobSourceID = " + str(id_) + ";")
        self.conn.commit()
        
        self.PrintToHtml('Save jobdetailClean to DB: '+str(id_))
        
    def writeJobDetailKeyword(self, id_, keyWordString_):
        #ALTER TABLE JobListings CONVERT TO CHARACTER SET utf8
        
        self.cur.execute("INSERT INTO JobDetailKey (JobSourceID, KeyWord) values ('"+str(id_)+"','"+str(keyWordString_)+"');")
        #UPDATE JobDetailKey SET KeyWord = '" + keyWordString_ + "' WHERE JobSourceID = " + str(id_) + ";")
        self.conn.commit()
        
        self.PrintToHtml('Updated KeyWordString: '+str(id_))
        
    def readJobDetailKeyword(self):
        self.cur.execute("SELECT * FROM ReportKeyWordQueryDate;")
        return(self.cur.fetchall())
        
    def readJobDetailClean(self,index_):
        self.cur.execute("SELECT JobSourceID, JobDetailClean FROM JobDetail ORDER BY JobSourceID LIMIT " + str(index_) + ", 1;")
        return(self.cur.fetchall())   
        
    def getNoJobs(self):
        self.cur.execute("select count(JobSourceID) from JobDetail")
        return(self.cur.fetchall())
        
    def writeKeyWord(self,keyWord_, Count_):
        #ALTER TABLE JobListings CONVERT TO CHARACTER SET utf8
        #keyWord_ = keyWord_.replace("'", "")
        
        self.cur.execute("INSERT IGNORE INTO KeyWords (Word, Count) VALUES ('"+keyWord_+"',"+str(Count_)+");")
        self.conn.commit()
        
        self.PrintToHtml('Save KeyWord to DB: '+str(keyWord_))
        
    def getKeyWord(self):
        self.cur.execute("SELECT Word FROM KeyWords WHERE NOT KeyWord IS NULL ORDER BY Count desc;")
        return(self.cur.fetchall())  
        
    def PrintToHtml(self,message):
        st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    
        with open('debug.html', 'a') as myfile:
            myfile.write('<p>' +str(st) + " | " + message + '</p>')