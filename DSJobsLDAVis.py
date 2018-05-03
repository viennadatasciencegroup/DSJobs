#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  3 03:21:04 2018

@author: user
"""

from pyLDAvis import gensim_#

with open('data/ldavis_prepared', 'rb') as f:
  LDAvis_prepared = pickle.load(f)
        
  pyLDAvis.show(LDAvis_prepared)
    