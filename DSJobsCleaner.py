#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 03:29:05 2018

@author: user
"""
from DSJobsDB import Job
import time
import datetime
from bs4 import BeautifulSoup
#from bs4 import UnicodeDammit

from textblob import TextBlob
import re
import csv

def JobDetailClean2(text_):
    soup=BeautifulSoup(text_).get_text()
    text_ = soup
    #text_ = text_.replace('\\/','/')

    with open('data/replacements/clean_karriere_at.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['Ignore']:
                continue
            if row['Regex']:
                text_ = re.sub(row['From'], row['To'], text_).strip()
            else:
                text_ = text_.replace(row['From'], row['To'])
    
    text_ = TextBlob(text_)
        
    if len(text_) > 3:
        if text_.detect_language() == 'de':
            text_ = text_.translate(to="en")
        
    return(str(text_))
    
def CleanAllJobs():
    a = 0
    with Job() as db:
        max_ = db.getNoJobs()
        while (a < int(max_[0][0])):
            #print(db.readJobDetail(a)[0][1])
            jobClean_ = JobDetailClean2(db.readJobDetail(a)[0][1])
            id_ = int(db.readJobDetail(a)[0][0])
            #print(id_,jobClean_)
            db.writeJobDetailClean(id_, jobClean_)

            a += 1
            PrintToHtml('Clean job No: ' + str(a) + ' | ' + str(id_) +' | of ' + str(max_[0][0]))
            
def PrintToHtml(message):

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    with open('job.html', 'a') as myfile:
        myfile.write('<p>' +str(st) + " | " + message + '</p>')
        
def main():
    '''
    update the database
    
    '''
    
    with open('job.html', 'w') as myfile:
        myfile.write('<p>Start Cleaning</p>')
    
    CleanAllJobs()
    
if __name__ == "__main__":
    main()
        
