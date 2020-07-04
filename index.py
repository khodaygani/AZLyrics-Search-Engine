from collections import Counter
from langdetect import detect
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer
from pymongo import MongoClient
from pymongo import ReturnDocument
from song import Song

def index():
    """
    This function parse the documents into songs collection in the remote datasource
    and create the index of the terms after normalizing and stemming the terms.
    Stopwords are removed.
    """
    # this handle the connection to the remote instance of mongo db
    #client = MongoClient('mongodb://sse:^#hO3^njQz3V@127.0.0.1:27017/sse')
    client = MongoClient('mongodb://sse:^#hO3^njQz3V@ds113586.mlab.com:13586/sse')
    sse = client.sse
    songs = sse.songs
    index_collection = sse.index

    vocabulary = {"_id":"vocabulary"}
    terms = {}
    # this tokenizer remove the punctuations
    tokenizer = RegexpTokenizer(r'\w+')
    total = songs.count()
    counter = 0
    for song in songs.find():
        counter += 1
        # lets try to understand the language and set the stopwords and stemmer accordingly
        try:
            lang = str.lower(ISO639_2[detect(song['Lyric'])])
            stop = set(stopwords.words(lang))
            stemmer = SnowballStemmer(lang)
        except:
            lang = "english"
            stop = set(stopwords.words(lang))
            stemmer = SnowballStemmer(lang)

        # we use the Counter collection to determine the frequency of the normalized term
        song_terms = [stemmer.stem(term) for term in tokenizer.tokenize(song['Lyric'].lower()) if term not in stop and term.isalpha() and len(term) > 1]
        song_tf = Counter(song_terms)
        
        print("%5d / %d - Indexing %s by %s"%(counter, total, song["Title"], song["Artist"]))
        
        # Append a reference for song containing that term and compute the term frequency and normalized term frequency.
        for term in song_tf:
            try:
                doc_list = terms[term]
            except KeyError:
                terms[term] = []
                doc_list = terms[term]
            doc_list.append({
                "_id":str(song['_id']),
                "tf": song_tf[term],
                "tf_norm": song_tf[term]/len(song_terms)
            })

    # Store the term document into the index collection
    total = len(terms)
    counter = 0
    for term_name, doc_list in terms.items():
        counter += 1
        term_id = index_collection.insert_one({
            "_id"  : term_name,
            "docs" : doc_list
        })
        
        vocabulary[term_name] = term_name
        print("%5d / %d - Term %s stored in vocabulary"%(counter, total, term_name))
    
    # Store the vocabulary into the index collection
    index_collection.insert_one(vocabulary)

if __name__ == "__main__":
    index()