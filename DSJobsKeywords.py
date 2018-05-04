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
o NLP Jupyter: http://nbviewer.jupyter.org/github/skipgram/modern-nlp-in-python/blob/master/executable/Modern_NLP_in_Python.ipynb

o https://gist.github.com/denjn5/404a99cd494942fe97b36e773d9b147a
'''

#import pymysql

from DSJobsDB import Job
import time
import datetime
import warnings
from collections import Counter

import spacy

nlp = spacy.load('en')
    
def PrintToHtml(message):

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    with open('job.html', 'a') as myfile:
        myfile.write('<p>' +str(st) + " | " + message + '</p>')

def WriteKeywords():
    trigram_reviews = open('data/trigram_transformed_reviews_all.txt', 'r')
    #trigram_reviews = open('data/review_text_all.txt', 'r')
    
    #t = nlp(str(trigram_reviews.read()))
    #print(t.count_by(ORTH))
    
    doc = nlp(trigram_reviews.read())
    # all tokens that arent stop words or punctuations
    words = [token.text for token in doc]# if token.is_stop != True and token.is_punct != True]

    word_freq = Counter(words)
    
    print(word_freq)
    
    if 0 == 1:
      with Job() as db:
          for word in word_freq:
              #print(word,word_freq[word])
              if (int(word_freq[word]) > 1):
                  db.writeKeyWord(str(word),int(word_freq[word]))
              
def KeywordsInJob():
    '''
    read list of keywords
    '''
    with Job() as db:
        keyWords_ = db.getKeyWord()
        for word_ in keyWords_:
            print(word_[0])
                
        a = 0
        while (a < db.getNoJobs()[0][0]):
            job_ = db.readJobDetailClean(a)
            #keyString_ = ''
            for word in keyWords_:
                if (str(job_[0]).find(str(word[0]).replace("_"," ").lower())>-1):
                    keyString_ = str(word[0])                    
                    db.writeJobDetailKeyword(job_[0][0], keyString_)
                    print(str(a)+' '+keyString_)
            a+=1

def main():
    '''
    update the database
    '''
    warnings.simplefilter("ignore", DeprecationWarning)
    
    #WriteKeywords()
    
    KeywordsInJob()
    
if __name__ == "__main__":
    main()