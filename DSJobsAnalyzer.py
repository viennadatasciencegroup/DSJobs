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
#from collections import Counter

import spacy
from spacy.lang.en.stop_words import STOP_WORDS
#import pandas as pd
import itertools as it

from gensim.models import Phrases
from gensim.models.word2vec import LineSentence

from gensim.corpora import Dictionary, MmCorpus
from gensim.models.ldamulticore import LdaMulticore

import pyLDAvis #conda install -c mlgill pyldavis
#import pyLDAvis.gensim
from pyLDAvis import gensim_# as ldavis
#import cPickle as pickle
import pickle

#import os
import codecs

nlp = spacy.load('en')
    
def PrintToHtml(message):

    st = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    with open('job.html', 'a') as myfile:
        myfile.write('<p>' +str(st) + " | " + message + '</p>')

def punct_space(token):
    """
    helper function to eliminate tokens
    that are pure punctuation or whitespace
    """
    
    return token.is_punct or token.is_space

def line_review(filename):
    """
    generator function to read in reviews from the file
    and un-escape the original line breaks in the text
    """
    
    with codecs.open(filename, encoding='utf_8') as f:
        for review in f:
            yield review.replace('\\n', '\n')
            
def lemmatized_sentence_corpus(filename):
    """
    generator function to use spaCy to parse reviews,
    lemmatize the text, and yield sentences
    """
    
    for parsed_review in nlp.pipe(line_review(filename), batch_size=10000, n_threads=4):
        
        for sent in parsed_review.sents:
            yield u' '.join([token.lemma_ for token in sent
                             if not punct_space(token)])

def trigram_bow_generator(trigram_dictionary,filepath):
    """
    generator function to read reviews from a file
    and yield a bag-of-words representation
    """
    
    for review in LineSentence(filepath):
        yield trigram_dictionary.doc2bow(review)
        
def explore_topic(lda, topic_number, topn=25):
    """
    accept a user-supplied topic number and
    print out a formatted list of the top terms
    """
        
    print(u'{:20} {}'.format(u'term', u'frequency') + u'\n')

    for term, frequency in lda.show_topic(topic_number, topn=25):
        print(u'{:20} {:.3f}'.format(term, round(frequency, 3)))
    
def get_sample_review(review_number):
    """
    retrieve a particular review index
    from the reviews file and return it
    """
    
    return list(it.islice(line_review('data/review_text_all.txt'), review_number, review_number+1))[0]

def lda_description(bigram_model, trigram_model, trigram_dictionary, lda, topic_names, review_text, min_topic_freq=0.05):
    """
    accept the original text of a review and (1) parse it with spaCy,
    (2) apply text pre-proccessing steps, (3) create a bag-of-words
    representation, (4) create an LDA representation, and
    (5) print a sorted list of the top topics in the LDA representation
    """
    
    # parse the review text with spaCy
    parsed_review = nlp(review_text)
    
    # lemmatize the text and remove punctuation and whitespace
    unigram_review = [token.lemma_ for token in parsed_review
                      if not punct_space(token)]
    
    # apply the first-order and secord-order phrase models
    bigram_review = bigram_model[unigram_review]
    trigram_review = trigram_model[bigram_review]
    
    # remove any remaining stopwords
    trigram_review = [term for term in trigram_review
                      if not term in STOP_WORDS]#spacy.en.STOPWORDS] # Echeck
    
    # create a bag-of-words representation
    review_bow = trigram_dictionary.doc2bow(trigram_review)
    
    # create an LDA representation
    review_lda = lda[review_bow]
    
    # sort with the most highly related topics first
    #review_lda = sorted(review_lda, key=lambda (topic_number, freq): -freq)
    
    for topic_number, freq in review_lda:
        if freq < min_topic_freq:
            break
            
        # print the most highly related topic names and frequencies
        print('{:25} {}'.format(topic_names[topic_number], round(freq, 3)))

def LDA_Analysis():
    #http://nbviewer.jupyter.org/github/skipgram/modern-nlp-in-python/blob/master/executable/Modern_NLP_in_Python.ipynb

    if 0 == 1:
        with open('data/review_text_all.txt','w') as myfile:
            myfile.write("")
        
        '''
        loop through db and write jobs descriptions
        '''
        
        with open('data/review_text_all.txt','a') as myfile:
            with Job() as db:
                a=0
                max_ = int(db.getNoJobs()[0][0])
                while (a < max_):
                    #print(a)
                    sample_review = db.readJobDetailClean(a)[0][1]
                    if (sample_review != 'Json Error'):
                        myfile.write(str(sample_review)+'\n')
                    a += 1
    
    #unigram_sentences_filepath = os.path.join(intermediate_directory, 'unigram_sentences_all.txt')
    
    if 0 == 1:
    
        with codecs.open('data/unigram_sentences_all.txt', 'w', encoding='utf_8') as f:
            for sentence in lemmatized_sentence_corpus('data/review_text_all.txt'):
                f.write(sentence + '\n')
    
    unigram_sentences = LineSentence('data/unigram_sentences_all.txt')
   
    '''
    for unigram_sentence in it.islice(unigram_sentences, 230, 240):
        print(u' '.join(unigram_sentence))
        print(u'')
    '''
        
    #bigram_model_filepath = os.path.join(intermediate_directory, 'bigram_model_all')
    
    if 0 == 1:

        bigram_model = Phrases('data/unigram_sentences_all.txt')
    
        bigram_model.save('data/bigram_model_all')
    
    # load the finished model from disk
    bigram_model = Phrases.load('data/bigram_model_all')
    
    #bigram_sentences_filepath = os.path.join(intermediate_directory, 'bigram_sentences_all.txt')
   
    if 0 == 1:
    
        with codecs.open('data/bigram_sentences_all.txt', 'w', encoding='utf_8') as f:
            
            for unigram_sentence in unigram_sentences:
                
                bigram_sentence = u' '.join(bigram_model[unigram_sentence])
                
                f.write(bigram_sentence + '\n')
            
    bigram_sentences = LineSentence('data/bigram_sentences_all.txt')
            
    '''                    
    for bigram_sentence in it.islice(bigram_sentences, 230, 240):
        print(u' '.join(bigram_sentence))
        print(u'')  
    '''

    #trigram_model_filepath = os.path.join(intermediate_directory, 'trigram_model_all')

    if 0 == 1:
    
        trigram_model = Phrases(bigram_sentences)
    
        trigram_model.save('data/trigram_model_all')
        
    # load the finished model from disk
    trigram_model = Phrases.load('data/trigram_model_all')

    #trigram_sentences_filepath = os.path.join(intermediate_directory, 'trigram_sentences_all.txt')                     

    if 0 == 1:
    
        with codecs.open('data/trigram_sentences_all.txt', 'w', encoding='utf_8') as f:
            
            for bigram_sentence in bigram_sentences:
                
                trigram_sentence = u' '.join(trigram_model[bigram_sentence])
                
                f.write(trigram_sentence + '\n')
                
    trigram_sentences = LineSentence('data/trigram_sentences_all.txt')

    '''
    for trigram_sentence in it.islice(trigram_sentences, 230, 240):
        print(u' '.join(trigram_sentence))
        print(u'')
    '''

    #trigram_reviews_filepath = os.path.join(intermediate_directory, 'trigram_transformed_reviews_all.txt')
    
    if  0 == 1:
    
        with codecs.open('data/trigram_transformed_reviews_all.txt', 'w', encoding='utf_8') as f:
            
            for parsed_review in nlp.pipe(line_review('data/review_text_all.txt'), batch_size=10000, n_threads=4):
                
                # lemmatize the text, removing punctuation and whitespace
                unigram_review = [token.lemma_ for token in parsed_review
                                  if not punct_space(token)]
                
                # apply the first-order and second-order phrase models
                bigram_review = bigram_model[unigram_review]
                trigram_review = trigram_model[bigram_review]
                
                # remove any remaining stopwords
                trigram_review = [term for term in trigram_review
                                  if term not in  STOP_WORDS]#spacy.en.STOPWORDS] !!!!! CHECK THIS !!!!! module 'spacy' has no attribute 'en'
                
                # write the transformed review as a line in the new file
                trigram_review = u' '.join(trigram_review)
                f.write(trigram_review + '\n')
                
    '''
    print(u'Original:' + u'\n')
    
    for review in it.islice(line_review('review_text_all.txt'), 11, 12):
        print(review)
    
    print(u'----' + u'\n')
    print(u'Transformed:' + u'\n')
    
    with codecs.open('trigram_transformed_reviews_all.txt', encoding='utf_8') as f:
        for review in it.islice(f, 11, 12):
            print(review)
    '''

    #trigram_dictionary_filepath = os.path.join(intermediate_directory, 'trigram_dict_all.dict')

    if 0 == 1:
    
        trigram_reviews = LineSentence('data/trigram_transformed_reviews_all.txt')
    
        # learn the dictionary by iterating over all of the reviews
        trigram_dictionary = Dictionary(trigram_reviews)
        
        # filter tokens that are very rare or too common from
        # the dictionary (filter_extremes) and reassign integer ids (compactify)
        trigram_dictionary.filter_extremes(no_below=10, no_above=0.4)#,keep_n=100000)#,)
        trigram_dictionary.compactify()
    
        trigram_dictionary.save('data/trigram_dict_all.dict')
        
    # load the finished dictionary from disk
    trigram_dictionary = Dictionary.load('data/trigram_dict_all.dict')
    
    #trigram_bow_filepath = os.path.join(intermediate_directory, 'trigram_bow_corpus_all.mm')
    
    if 0 == 1:
    
        # generate bag-of-words representations for
        # all reviews and save them as a matrix
        MmCorpus.serialize('data/trigram_bow_corpus_all.mm', trigram_bow_generator(trigram_dictionary,'data/trigram_transformed_reviews_all.txt'))
        
    # load the finished bag-of-words corpus from disk
    trigram_bow_corpus = MmCorpus('data/trigram_bow_corpus_all.mm')
    
    #lda_model_filepath = os.path.join(intermediate_directory, 'lda_model_all')
    
    if 0 == 1:
    
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            
            # workers => sets the parallelism, and should be
            # set to your number of physical cores minus one
            lda = LdaMulticore(trigram_bow_corpus,
                               num_topics=200,
                               id2word=trigram_dictionary,
                               workers=1)
        
        lda.save('data/lda_model_all')
        
    # load the finished LDA model from disk
    lda = LdaMulticore.load('data/lda_model_all')

    explore_topic(lda, topic_number=1)

    topic_names = {0:u'Analysis International Medical', 
                   1:u'Group Security', 
                   2:u'Topic 3', 
                   3:u'Data Analysis Architecture', 
                   4:u'Test Analysis Tool', 
                   5:u'Topic 6', 
                   6:u'Topic 7', 
                   7:u'Topic 8', 
                   8:u'Topic 9', 
                   9:u'Topic 10', 
                   10:u'Topic 11',
                   11:u'Topic 12', 
                   12:u'Topic 13', 
                   13:u'Topic 14', 
                   14:u'Topic 15', 
                   15:u'Topic 16', 
                   16:u'Topic 17', 
                   17:u'Topic 18', 
                   18:u'Topic 19', 
                   19:u'Topic 20', 
                   20:u'Topic 21',
                   21:u'Topic 22', 
                   22:u'Topic 23', 
                   23:u'Topic 24', 
                   24:u'Topic 25',
                   25:u'Analysis International Medical', 
                   26:u'Group Security', 
                   27:u'Topic 3', 
                   28:u'Data Analysis Architecture', 
                   29:u'Test Analysis Tool', 
                   30:u'Topic 6', 
                   31:u'Topic 7', 
                   32:u'Topic 8', 
                   33:u'Topic 9', 
                   34:u'Topic 10', 
                   35:u'Topic 11',
                   36:u'Topic 12', 
                   37:u'Topic 13', 
                   38:u'Topic 14', 
                   39:u'Topic 15', 
                   40:u'Topic 16', 
                   41:u'Topic 17', 
                   42:u'Topic 18', 
                   43:u'Topic 19', 
                   44:u'Topic 20', 
                   45:u'Topic 21',
                   46:u'Topic 22', 
                   47:u'Topic 23', 
                   48:u'Topic 24', 
                   49:u'Topic 25',
                   50:u'Analysis International Medical', 
                   51:u'Group Security', 
                   52:u'Topic 3', 
                   53:u'Data Analysis Architecture', 
                   54:u'Test Analysis Tool', 
                   55:u'Topic 6', 
                   56:u'Topic 7', 
                   57:u'Topic 8', 
                   58:u'Topic 9', 
                   59:u'Topic 10', 
                   60:u'Topic 11',
                   61:u'Topic 12', 
                   62:u'Topic 13', 
                   63:u'Topic 14', 
                   64:u'Topic 15', 
                   65:u'Topic 16', 
                   66:u'Topic 17', 
                   67:u'Topic 18', 
                   68:u'Topic 19', 
                   69:u'Topic 20', 
                   70:u'Topic 21',
                   71:u'Topic 22', 
                   72:u'Topic 23', 
                   73:u'Topic 24', 
                   74:u'Topic 25',
                   75:u'Analysis International Medical', 
                   76:u'Group Security', 
                   77:u'Topic 3', 
                   78:u'Data Analysis Architecture', 
                   79:u'Test Analysis Tool', 
                   80:u'Topic 6', 
                   81:u'Topic 7', 
                   82:u'Topic 8', 
                   83:u'Topic 9', 
                   84:u'Topic 10', 
                   85:u'Topic 11',
                   86:u'Topic 12', 
                   87:u'Topic 13', 
                   88:u'Topic 14', 
                   89:u'Topic 15', 
                   90:u'Topic 16', 
                   91:u'Topic 17', 
                   92:u'Topic 18', 
                   93:u'Topic 19', 
                   94:u'Topic 20', 
                   95:u'Topic 21',
                   96:u'Topic 22', 
                   97:u'Topic 23', 
                   98:u'Topic 24', 
                   99:u'Topic 25',
                   100:u'Analysis International Medical', 
                   101:u'Group Security', 
                   102:u'Topic 3', 
                   103:u'Data Analysis Architecture', 
                   104:u'Test Analysis Tool', 
                   105:u'Topic 6', 
                   106:u'Topic 7', 
                   107:u'Topic 8', 
                   108:u'Topic 9', 
                   109:u'Topic 10', 
                   110:u'Topic 11',
                   111:u'Topic 12', 
                   112:u'Topic 13', 
                   113:u'Topic 14', 
                   114:u'Topic 15', 
                   115:u'Topic 16', 
                   116:u'Topic 17', 
                   117:u'Topic 18', 
                   118:u'Topic 19', 
                   119:u'Topic 20', 
                   120:u'Topic 21',
                   121:u'Topic 22', 
                   122:u'Topic 23', 
                   123:u'Topic 24', 
                   124:u'Topic 25',
                   125:u'Analysis International Medical', 
                   126:u'Group Security', 
                   127:u'Topic 3', 
                   128:u'Data Analysis Architecture', 
                   129:u'Test Analysis Tool', 
                   130:u'Topic 6', 
                   131:u'Topic 7', 
                   132:u'Topic 8', 
                   133:u'Topic 9', 
                   134:u'Topic 10', 
                   135:u'Topic 11',
                   136:u'Topic 12', 
                   137:u'Topic 13', 
                   138:u'Topic 14', 
                   139:u'Topic 15', 
                   140:u'Topic 16', 
                   141:u'Topic 17', 
                   142:u'Topic 18', 
                   143:u'Topic 19', 
                   144:u'Topic 20', 
                   145:u'Topic 21',
                   146:u'Topic 22', 
                   147:u'Topic 23', 
                   148:u'Topic 24', 
                   149:u'Topic 25',
                   150:u'Analysis International Medical', 
                   151:u'Group Security', 
                   152:u'Topic 3', 
                   153:u'Data Analysis Architecture', 
                   154:u'Test Analysis Tool', 
                   155:u'Topic 6', 
                   156:u'Topic 7', 
                   157:u'Topic 8', 
                   158:u'Topic 9', 
                   159:u'Topic 10', 
                   160:u'Topic 11',
                   161:u'Topic 12', 
                   162:u'Topic 13', 
                   163:u'Topic 14', 
                   164:u'Topic 15', 
                   165:u'Topic 16', 
                   166:u'Topic 17', 
                   167:u'Topic 18', 
                   168:u'Topic 19', 
                   169:u'Topic 20', 
                   170:u'Topic 21',
                   171:u'Topic 22', 
                   172:u'Topic 23', 
                   173:u'Topic 24', 
                   174:u'Topic 25',
                   175:u'Analysis International Medical', 
                   176:u'Group Security', 
                   177:u'Topic 3', 
                   178:u'Data Analysis Architecture', 
                   179:u'Test Analysis Tool', 
                   180:u'Topic 6', 
                   181:u'Topic 7', 
                   182:u'Topic 8', 
                   183:u'Topic 9', 
                   184:u'Topic 10', 
                   185:u'Topic 11',
                   186:u'Topic 12', 
                   187:u'Topic 13', 
                   188:u'Topic 14', 
                   189:u'Topic 15', 
                   190:u'Topic 16', 
                   191:u'Topic 17', 
                   192:u'Topic 18', 
                   193:u'Topic 19', 
                   194:u'Topic 20', 
                   195:u'Topic 21',
                   196:u'Topic 22', 
                   197:u'Topic 23', 
                   198:u'Topic 24', 
                   199:u'Topic 25'}
    
    #topic_names_filepath = os.path.join(intermediate_directory, 'topic_names.pkl')
    
    with open('data/topic_names.pkl', 'wb') as f:
        pickle.dump(topic_names, f)
        
    sample_review = get_sample_review(10)
    
    lda_description(bigram_model, trigram_model, trigram_dictionary, lda, topic_names, sample_review)

    #LDAvis_data_filepath = os.path.join(intermediate_directory, 'ldavis_prepared')
    
    if 0 == 1:
        
        #term_ix = np.sort(topic_info.index.unique().values)
    
        LDAvis_prepared = pyLDAvis.gensim_.prepare(lda, trigram_bow_corpus, trigram_dictionary)
    
        with open('data/ldavis_prepared', 'wb') as f:
            pickle.dump(LDAvis_prepared, f)
            
    # load the pre-prepared pyLDAvis data from disk
    with open('data/ldavis_prepared', 'rb') as f:
        LDAvis_prepared = pickle.load(f)
        
    pyLDAvis.show(LDAvis_prepared)
    
    #pyLDAvis.display(pyLDAvis.gensim_.prepare(lda, trigram_bow_corpus, trigram_dictionary))

    
def main():
    '''
    update the database
    '''
    #CleanAllJobs()
    warnings.simplefilter("ignore", DeprecationWarning)
    
    LDA_Analysis()
    
if __name__ == "__main__":
    main()