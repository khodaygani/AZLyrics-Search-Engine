from collections import Counter
from langdetect import detect
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

from heapq import heappush, nsmallest
from sklearn.cluster import KMeans
from math import floor
import matplotlib.pyplot as plt
import numpy
import sys
import math
from nltk.stem.snowball import SnowballStemmer
from pymongo import MongoClient
from wordcloud import WordCloud

def prepareSearch(word):
    # Normalize and stem the query string
    stemmer = SnowballStemmer('english')
    word = word.lower()
    word = stemmer.stem(word)
    return word

def computeIdf(total, occurence):
    # compute the idf of the query vector
    return math.log(total / occurence)

def find_songs_in_index(index_collection, words, total_songs):
    """
    This function performs the search into the index and returns the matching songs and the corresponding tfidf
    
    index_collection: The reference to the index collection on the datasource
    words: The list containing the query string
    total_songs: The total number of songs into the datasource
    """
    docs_tfidf = {}

    songs_found = []    
    for term in index_collection.find({"_id": {"$in":words}}):
        sxt = []
        for document in term["docs"]:
            docid = document["_id"]
            sxt.append(docid)
            try:
                docs_tfidf[docid].append(computeIdf(total_songs, len(term["docs"])*document["tf_norm"]))
            except KeyError:
                docs_tfidf[docid] = []
                docs_tfidf[docid].append(computeIdf(total_songs, len(term["docs"])*document["tf_norm"]))
        songs_found.append(sxt)
    return docs_tfidf, songs_found

def ask_query_type():
    # Ask the user for the type os search to be performed
    while True:
        try:
            search_type = int(input("Insert query type 1 or 2: "))
        except ValueError:
            print("Sorry, I didn't understand that. Only 1 or 2 are allowed.")
            continue
        else:
            if search_type not in [1,2]:
                continue
            else:
                break
    return search_type

def ask_number_of_cluster(maximum):
    # Ask user for the number of cluster to be created
    while True:
        try:
            k = int(input("Enter a value to clusterize the results:"))
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue
        else:
            if k <= maximum and k > 0:
                break
            else:
                print("Sorry, you can create up to %d clusters."%maximum)
                continue
    return k

def search_index(search_type, words, docs_tfidf, songs_found):
    """
    This function executes the search into the index and returns the list of founded songs.
    
    search_type: The type of search that has to be performed
    words: the list containing the query string
    docs_tfidf: the tfidf of the matching documents
    songs_found: the list of matching documents found
    """
    # preparing search query vector
    query_tf_idf = [1/len(words)]* len(words)
    q_norm = math.sqrt(sum(list(map(lambda x: x**2, query_tf_idf))))
    
    query_type = []
    query_type.append(lambda lists:set([ x for y in lists for x in y]))
    query_type.append(lambda lists:list(set.intersection(*map(set, lists))))
    
    if search_type == 1:
        heap = []
        top10= []
        for doc, tfidf in docs_tfidf.items():
            if doc in query_type[search_type-1](songs_found):
                dot_p = 0
                for i in range(len(tfidf)):
                    dot_p += tfidf[i]*query_tf_idf[i]
                d_norm = math.sqrt(sum(list(map(lambda x: x**2, tfidf))))
                cos = dot_p / (d_norm*q_norm)
                heappush(heap, (1-cos, doc))
                top10 = nsmallest(10, heap)

        return top10    
    else:
        match = []
        for doc, tfidf in docs_tfidf.items():
            if doc in query_type[search_type-1](songs_found):
                dot_p = 0
                for i in range(len(tfidf)):
                    dot_p += tfidf[i]*query_tf_idf[i]
                d_norm = math.sqrt(sum(list(map(lambda x: x**2, tfidf))))
                cos = dot_p / (d_norm*q_norm)
                match.append((1-cos, doc))
        
        print("%d matching songs found!"%len(match))
        # exit if no songs were found
        if len(match) == 0:
            sys.exit(0)
        return match    


def create_clusters(songs_found, docs_tfidf, k, songs_collection):
    """
    This function returns the clusters of songs using the K-Mean Algorithm.
    
    songs_found: The list of songs returned by the search
    docs_tfidf: The list that contains the similarity of the songs with the query string
    k: The number of cluser to be created
    songs_collection: The reference to the songs collection on the datasource
    """
    lang = "english"
    stop = set(stopwords.words(lang))   
    tokenizer = RegexpTokenizer(r'\w+')
        
    clusters_res = {}
    tfidf_matrix = []
    songs_list = []
    for entry in songs_found:
        songs_list.append(entry[1])
        tfidf_matrix.append(docs_tfidf[entry[1]])
        
    # Here we use the K-Mean algoorithm to split the results in clusters
    print("Creating %d clusters"%k)
    km = KMeans(n_clusters=k)
    km.fit(tfidf_matrix)
    c = km.predict(tfidf_matrix)
    clusters = {}
    for i in range(k):
        clusters[i] = []
    for i in range(len(c)):
        clusters[c[i]].append(songs_list[i])
    for i in range(k):
        clusters_res[i] = {"songs":[], "terms":[]}
        for s_id in clusters[i]:
            song = songs_collection.find_one({"_id":int(s_id)})
            clusters_res[i]["songs"].append({"Title":song["Title"], "Artist": song["Artist"]})
            clusters_res[i]["terms"] += [term for term in tokenizer.tokenize(song['Lyric'].lower()) if term not in stop and term.isalpha() and len(term) > 1]

    return clusters_res
    
def usage():
    print("Usage: python search.py <query string>")
    
def search(words):
    """
    This function execute the search in the index.
    
    words: list of word to be searched
    """
    
    # Set up the connection to the remote datasource
    client = MongoClient('mongodb://user:pass@ds113586.mlab.com:13586/sse')

    sse = client.sse
    songs_collection = sse.songs
    index_collection = sse.index
    total_songs = songs_collection.count()
    
    words = list(map(prepareSearch, words))

    # lookup the index for matching documents
    docs_tfidf, songs_found = find_songs_in_index(index_collection, words, total_songs)
    
    #Exit if no songs are found
    if len(songs_found) == 0:
        print("No songs found, try with a different query string!")
        sys.exit(0)
        
    # preparing search query vector
    query_tf_idf = [1/len(words)]* len(words)
    q_norm = math.sqrt(sum(list(map(lambda x: x**2, query_tf_idf))))

    search_type = ask_query_type()

    if search_type == 1:
        # Prints the Top10 songs that contains the query string
        top10 = search_index(1, words, docs_tfidf, songs_found)
        for findings in top10:
            song = songs_collection.find_one({"_id":int(findings[1])})
            print("%f %s by %s"%(1-findings[0],song["Title"],song["Artist"]))
        
    else:
        # Create clusters using the songs that contains all the words in the query string
        match = search_index(2, words, docs_tfidf, songs_found)
        k = ask_number_of_cluster(maximum=len(match))
        clusters = create_clusters(match, docs_tfidf, k, songs_collection)
        
        # Print the Cloud of Words of each cluster
        for cid, res in clusters.items():
            print("Cluster #%d"%cid)

            for song in res["songs"]:
                print("%s by %s"%(song["Title"],song["Artist"]))

            counter = Counter(res["terms"])
            # To visualize only the most commons term we filter the list of term including the ones that
            # occurs at least 1/4 of the most common term in the collection
            most_common = [term for term in counter.elements() if counter[term] > counter.most_common(1)[0][1]/4]
            wordcloud = WordCloud(collocations = False).generate(' '.join(most_common))
            plt.imshow(wordcloud, interpolation='bilinear', aspect='auto')
            plt.axis("off")
            plt.show()
        
if __name__ == "__main__":
    words = sys.argv[1:]
    if len(words) == 0:
        usage()
        sys.exit(1)    
    search(words)
