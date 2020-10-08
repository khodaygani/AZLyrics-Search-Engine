from pymongo import MongoClient
from song import Song, SongFactory
from os import listdir
from bs4 import BeautifulSoup as BS
import requests
import time
import string
import numpy as np
import re
import urllib2
import markupsafe
def getSongsURLs(mypath):
    # this function returns a list of URLs
    htmls = listdir(mypath)
    
    return htmls
    
def lyrics_parser():
    """
    This function parse the HTML files containing the songs from azlyrics
    and add the resulting document into the songs collection in remote datasource
    """
    # this handle the connection to the remote instance of mongo db
    client = MongoClient('mongodb://sse:^#hO3^njQz3V@ds113586.mlab.com:13586/sse')
    #client = MongoClient('mongodb://sse:^#hO3^njQz3V@127.0.0.1:27017/sse')
    sse = client.sse
    songs = sse.songs

    # Retrieve the list of songs' urls
    #mypath = "./test/"
    mypath = "./shayt/lyrics_collection/"
    htmls = getSongsURLs(mypath)
    counter = 0
    for lyricHTML in htmls:
        # return an instance of Song
        try:
            counter += 1
            lyric = SongFactory.parse(mypath + lyricHTML)

            # store the song into the remote mongodb
            l = lyric.__dict__
            l["URL"] = lyricHTML
            l["_id"] = counter
            if counter % 2 == 0:
                song_id = songs.insert_one(l).inserted_id
                print("Storing # %5d: %s"%(counter,lyric.Title))
            else:
                print("Skipping # %5d: %s"%(counter,lyric.Title))
        except:
            print("Error storing %s"%lyric.Title)

def getArtists():
    ch_list = list(string.ascii_lowercase)
    artists = []

    for ch in ch_list:
        url = "https://www.azlyrics.com/" + ch + ".html"
        headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
        time.sleep(np.random.uniform(1,2))
        response = requests.get(url, headers=headers)
        BSpage = BS(response.text, "html.parser")
        temp_list = BSpage.findAll('a')
        for n in range(len(temp_list)):
            if temp_list[n]["href"][:2] != '//':
                artists.append(temp_list[n]["href"])
                
    return artists

def scrape():
    artists = getArtists()
    for artist in artists:
        #Here I have only one page to scrape
        url = "https://www.azlyrics.com/" + artist 


        #This is the part we tell the website that we are not a robot
        headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
        response = requests.get(url, headers=headers)

        #We use Beautiful Soup to scrape
        BSpage = BS(response.text, "html.parser")

        #Here we are using findAll of beautifulsoup to find all the a tags which have an attribute of taget=_blank;
        #then, we select the first tag and the retrieve the href for the a tag.
        ##BSpage.findAll('a',{'target':'_blank'})[0]["href"]
        temp_list = BSpage.findAll('a',{'target':'_blank'})
        for n in range(len(temp_list)):
            songUrl = 'https://azlyrics.com/lyrics/'+ temp_list[n]["href"][10:]
            print ('https://azlyrics.com/lyrics/'+ temp_list[n]["href"][10:]) 
            #https://www.azlyrics.com/lyrics/evanescence/nevergoback.html
            response = urllib2.urlopen(songUrl)
            m = re.search(r"(\w+)\/(\w+.html)", songUrl)
            with open('./shayt/lyrics_collection/'+m.group(2).strip(), 'w') as the_file:
                the_file.write(response.read())               
            time.sleep(np.random.uniform(1,2))
            
    lyrics_parser()            
        
if __name__ == "__main__":
    scrape()