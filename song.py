import codecs
import re
from bs4 import BeautifulSoup as BS

class Song(object):
    def __init__(self):
        self.Title = None
        self.Artist = None
        self.Lyric = None
        self.URL = None
        
class SongFactory(object):
    def getArtist(BSpage):
        # this method returns the artist name
        temp_list = BSpage.find('title')
        pageTitle =  temp_list.get_text()
        m = re.search(r"(.+)Lyrics - (.+)", pageTitle)
        return m.group(2).strip()
    
    def getTitle(BSpage):
        # this method returns the title of the song
        temp_list = BSpage.find('title')
        pageTitle =  temp_list.get_text()
        m = re.search(r"(.+)Lyrics - (.+)", pageTitle)
        return m.group(1).strip()
    
    def getLyric(BSpage):
        # this method return the lyric
        temp_list = BSpage.find('div',{'id':'content_h'},{'class':'dn'})
        temp =  str(temp_list)
        lyrics = temp.replace('<br/>', " ")[31:-6]
        return lyrics
    
    def scrape(html):
        f=codecs.open(html, 'r')
        BSpage = BS(f, "html.parser")
        
        song = Song()
        song.Title = SongFactory.getTitle(BSpage)
        song.Artist = SongFactory.getArtist(BSpage)
        song.Lyric = SongFactory.getLyric(BSpage)
        song.URL = None
        return song
        
    scrape = staticmethod(scrape)    