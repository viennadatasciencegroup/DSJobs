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

import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.attrs import ORTH
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
    
    if  1 == 1:
      
        import csv
        
        with open('data/StopWords.csv', newline='') as csvfile:
          
          stopwords_ = csv.reader(csvfile, delimiter=' ', quotechar='|')
          for words_ in stopwords_:
            #print(words_[0])
            STOP_WORDS.add(words_[0])
    
        #print(STOP_WORDS)
    
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
                                  if term not in STOP_WORDS]#spacy.en.STOPWORDS] !!!!! CHECK THIS !!!!! module 'spacy' has no attribute 'en'
                
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

    if 1 == 1:
    
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
    
    if 1 == 1:
    
        # generate bag-of-words representations for
        # all reviews and save them as a matrix
        MmCorpus.serialize('data/trigram_bow_corpus_all.mm', trigram_bow_generator(trigram_dictionary,'data/trigram_transformed_reviews_all.txt'))
        
    # load the finished bag-of-words corpus from disk
    trigram_bow_corpus = MmCorpus('data/trigram_bow_corpus_all.mm')
    
    #lda_model_filepath = os.path.join(intermediate_directory, 'lda_model_all')
    
    if 1 == 1:
    
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            
            # workers => sets the parallelism, and should be
            # set to your number of physical cores minus one
            lda = LdaMulticore(trigram_bow_corpus,
                               num_topics=10,
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
                   25:u'Analysis International Medical'}
    
    #topic_names_filepath = os.path.join(intermediate_directory, 'topic_names.pkl')
    
    with open('data/topic_names.pkl', 'wb') as f:
        pickle.dump(topic_names, f)
        
    sample_review = get_sample_review(10)
    
    lda_description(bigram_model, trigram_model, trigram_dictionary, lda, topic_names, sample_review)

    #LDAvis_data_filepath = os.path.join(intermediate_directory, 'ldavis_prepared')
    
    if 1 == 1:
        
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