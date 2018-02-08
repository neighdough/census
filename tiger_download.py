'''
Created on Jan 23, 2014

@author: nfergusn




'''

from bs4 import BeautifulSoup as bs
import urllib
import urllib2
import os

url_root = 'www2.census.gov/geo/tiger'
output_location = 's:\data\census\geo\\'
output_list = []
# response = urllib2.urlopen(url_root)
# html = response.read()



def set_soup(url):
    try:
        page = urllib2.urlopen(url)
        soup = bs(page.read())
        get_links(soup, url)
    except urllib2.URLError as e:
        output_list.append(('ERROR code: ', e.code, url))
        print 'ERROR code: ', e.code, url                         
 
def get_links(soup, url):
    for link in soup.find_all('pre')[0].find_all('a'):
        #
        if len(set(['Name', 'Last modified', 'Size', 'Description', 'Parent Directory']) & set(link.contents)) == 0:
            if not '.zip' in link.contents[0]:
                nxt_url = url + link.contents[0]
                output_list.append(('directory - ',nxt_url))
                set_soup(nxt_url)
            else:
                download_file(url, link.contents[0])
        elif link.contents[0] == 'Parent Directory':
            parent_directory = link.get('href')
            output_list.append(('parent_directory - ', parent_directory))          
            print 'parent_directory - ', parent_directory
                    
def download_file(parent_directory, link):
    try:
        outdir = parent_directory.split('/')[-2] if parent_directory.split('/')[-2] not in ['2000', '2010'] else parent_directory.split('/')[-3]
        if not os.path.exists(os.path.join(output_location, outdir)):
            os.mkdir(os.path.join(output_location, outdir))
        else:
            try:
                if not os.path.isfile(os.path.join(output_location, outdir, link)):
                    urllib.urlretrieve(parent_directory  + link, output_location + outdir + '/' + link)
                output_list.append(('\tDOWNLOAD ', outdir + '/' ,link))
                print '\tDOWNLOAD ', outdir + '/' ,link
            except urllib2.URLError as e:
                output_list.append(('ERROR code: ',e.code, parent_directory, link))
                print 'ERROR code: ',e.code, parent_directory, link
                
    except urllib2.URLError as e:
        output_list.append(('ERROR code: ', e.code, outdir, '/', link))
        print 'ERROR code: ', e.code, outdir, '/', link 

def write_status(output_list):
    with open(output_location + 'download_log.txt', 'w') as f:
        for line in output_list:
            f.write(line)
        
#set_soup('http://www2.census.gov/geo/tiger/TIGER2010/')
#write_status(output_list)

