from nltk.corpus import stopwords
import pymysql
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from pprint import pprint as pp
import pandas as pd


stops = list(stopwords.words("english"))

conn = pymysql.connect(host="localhost", user="root", passwd="root", db="dsjobsv2", use_unicode=True, charset="utf8")
cur = conn.cursor()

cur.execute("select JobDetailClean from JobDetail")

result = cur.fetchall()
corpus = []

for row in result:
    corpus.append(row[0])

vectorizer = CountVectorizer(stop_words=stops)

analyzer = vectorizer.build_analyzer()

#for idx, item in enumerate(corpus):
    #test = analyzer(item)

X = vectorizer.fit_transform(corpus)

counts = X.toarray()

transformer = TfidfTransformer(smooth_idf=False)
tfidf = transformer.fit_transform(counts)

data = tfidf.toarray()

df = pd.DataFrame(data=data, index=list(range(0, len(data))), columns=vectorizer.get_feature_names())

df.to_csv("./notebook/data.csv", sep=";")

if conn:
    conn.close()
