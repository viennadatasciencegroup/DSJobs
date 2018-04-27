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

def JobDetailClean2(text_):
    soup=BeautifulSoup(text_).get_text()
    text_ = soup
    #text_ = text_.replace('\\/','/')
    text_ = text_.replace('<\\/strong>','')
    text_ = text_.replace('<\\/p>','.')
    text_ = text_.replace('<\\/li>','.')
    text_ = text_.replace('<\\/ul>','.')
    text_ = text_.replace('<\\/h1>','.')
    text_ = text_.replace('<\\/h2>','.')
    text_ = text_.replace('<\\/h3>','.')
    text_ = text_.replace('<\\/h4>','.')
    text_ = text_.replace('<\\/h5>','.')
    text_ = text_.replace('\\n','.')
    text_ = text_.replace('\\/','/')
    
    text_ = text_.replace('bzw.','bzw')
    text_ = text_.replace('Mag.','Mag')
    text_ = text_.replace('Nr.','Nr')
    text_ = text_.replace('z.B.','zB')
    text_ = text_.replace('z.H.','zH')
    
    text_ = text_.replace('xc3xa4','ä')
    text_ = text_.replace('xc3x84','Ä')
    text_ = text_.replace('xc3xbc','ü')
    text_ = text_.replace('xc3x9c','Ü')
    text_ = text_.replace('xc3xb6','ö')
    text_ = text_.replace('xc3x96','Ö')
    text_ = text_.replace('xc3x9f','ß')
    
    text_ = text_.replace('"@type":','')
    text_ = text_.replace('"@context":','')
    text_ = text_.replace('"','')
    text_ = text_.replace('{','')
    text_ = text_.replace('}','')
    text_ = text_.replace('[','')
    text_ = text_.replace(']','')

    text_ = re.sub( '\.+', '. ', text_ ).strip()
    text_ = text_.replace(' . ','. ')
    text_ = text_.replace(',',', ')
    text_ = text_.replace(':',': ')
    text_ = re.sub( '\s+', ' ', text_ ).strip()
    
    text_ = TextBlob(text_)
        
    if len(text_) > 3:
        if (text_.detect_language() == 'de'):
            text_ = text_.translate(to="en")
        
    return(str(text_))

def JobDetailClean(text_):
    '''
    Get the json structure of the detailed job description and clean the text
    file. Use textblob to select sentences. remove the first and the last sentence and
    buil a new text file. If the file is in german, translage it to english
    
    http://textblob.readthedocs.io/en/dev/quickstart.html
    '''
    
    try:
        text_ = json.loads(text_)['description']#json.dumps(text_))
    
        text_ = text_.replace('<h1>','')
        text_ = text_.replace('<h2>','')
        text_ = text_.replace('<h3>','')
        text_ = text_.replace('<h4>','')
        text_ = text_.replace('<li>','')
        text_ = text_.replace('<ul>','')
        text_ = text_.replace('<p>','')
        text_ = text_.replace('<br>','')
        text_ = text_.replace('<strong>','')
        
        text_ = text_.replace('</h1>','.')
        text_ = text_.replace('</h2>','.')
        text_ = text_.replace('</h3>','.')
        text_ = text_.replace('</h4>','.')
        text_ = text_.replace(' </li>','.')
        text_ = text_.replace('</li>','.')
        text_ = text_.replace('</ul>','')
        text_ = text_.replace(' </p>','.')
        text_ = text_.replace('</p>','.')
        text_ = text_.replace('</strong>','')
        text_ = text_.replace('<br />','')
        
        text_ = text_.replace('..','.')
        text_ = text_.replace(':.',': ')
        
        text_ = text_.replace('\n',' ')
        
        text_ = text_.replace('  ',' ')
        text_ = text_.replace('  ',' ')
        text_ = text_.replace('  ',' ')
        text_ = text_.replace('  ',' ')
        text_ = text_.replace('  ',' ')
        
        text_ = text_.replace('bzw.','bzw')
        text_ = text_.replace('Mag.','Mag')
        text_ = text_.replace('Nr.','Nr')
        text_ = text_.replace('z.B.','zB')
        text_ = text_.replace('z.H.','zH')
        
        text_ = text_.replace('xc3xa4','ä')
        text_ = text_.replace('xc3x84','Ä')
        text_ = text_.replace('xc3xbc','ü')
        text_ = text_.replace('xc3x9c','Ü')
        text_ = text_.replace('xc3xb6','ö')
        text_ = text_.replace('xc3x96','Ö')
        text_ = text_.replace('xc3x9f','ß')
        
        text_ = text_.replace('xc2xbb','»')
        text_ = text_.replace('xe2x98','Θ')
            
        text_ = TextBlob(text_)
        
        if len(text_) > 3:
            if (text_.detect_language() == 'de'):
                text_ = text_.translate(to="en")

    except:
        text_='Json Error'
        
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
    
    #with Job() as db:
    #    print(db.readJobDetail(51)[0][1])
    #    print(JobDetailClean(db.readJobDetail(51)[0][1]))
    
if __name__ == "__main__":
    main()
        
