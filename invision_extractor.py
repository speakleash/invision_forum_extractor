from multiprocessing import set_start_method

from multiprocessing.pool import Pool
import requests
import glob
import shutil
import json
import os
from lm_dataformat import Archive
import urllib.robotparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib.parse
import time
from time import sleep
from bs4 import BeautifulSoup
from usp.tree import sitemap_tree_for_homepage

#CONFIG

DATASET_CATEGORY = "Dialogue"
DATASET_NAME = "forum_optymalizacja_com_corpus"
DATASET_DESCRIPTION = "Collection of forum discussions from forum.optymalizacja.com"
LICENSE = "(c) www.forum.optymalizacja.com"
DATASET_URL = 'https://www.forum.optymalizacja.com/'
EXPECTED_URL_PARTS = ['/topic','/temat']
PROCESSES=1

###


def urls_generator(url:str):

    tree = sitemap_tree_for_homepage(url)

    
    for page in tree.all_pages():
        if any(url_part in page.url for url_part in EXPECTED_URL_PARTS):
            yield page.url

    return

             

def get_item_text(url:str):
    
    response = None
    text = ''
    headers = {
		'User-Agent': 'Speakleash-v0.1',
		"Accept-Encoding": "gzip, deflate",
		"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Connection": "keep-alive"
	}
    try:
        response = session.get(url, timeout=60, headers=headers)

    except Exception as e:
        print("Error downloading -> "+url+" : "+str(e))
       

  

    if response and response.ok:

        if len(response.content)>15000000:
            print("File too big")
            return text
        
        soup = BeautifulSoup(response.content, "html.parser")
        comment_blocks = soup.find_all("div", {'data-role': "commentContent"})

        for comment in comment_blocks:
            text += comment.text.strip()+"\n"
        
        #Process next page
        try:
            
            time.sleep(0.2)
            
            while len(soup.find_all('li', {'class': 'ipsPagination_next'}))>0 and len(soup.find_all('li', class_='ipsPagination_next ipsPagination_inactive'))==0:
                
                next_page_url=soup.find_all('li', {'class': 'ipsPagination_next'})[0]['href']
                
                next_page_response = session.get(next_page_url, timeout=60, headers=headers)
                soup = BeautifulSoup(next_page_response.content, "html.parser")
                
                page_nav_results = soup.find_all("div", {'data-role': "commentContent"})

                for i in page_nav_results:
                    text += i.text.strip()+"\n"
                
        except Exception as e:
            print("ERROR processing next page "+next_page_url+" : "+str(e))
            
            
            
    elif not response:    
        print("Empty response -> "+url)
        print(response)

    elif not response.ok:
        print(response.status_code)
        print("Error -> "+url)
    
    return text
      
    
  
def process_item(url):

    item_url = url
    meta = {}
    txt = ''


    if rp.can_fetch('*', item_url):

        try:
            txt = get_item_text(item_url)
            
            if txt:

                l = len(txt.strip())
                sentences = words = verbs = nouns = punctuations = symbols = stopwords = oovs = 0
                meta = {'url' : item_url, 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols, 'stopwords': stopwords, 'oovs':oovs}
        except Exception as e:

            print("Error processing item -> "+item_url+" : "+str(e))
    else:
        print('Robots not allowed '+ item_url)


    return txt.strip(), meta

    

def initialize_worker(url:str):

    print('Initializing worker...')   

    global rp
    global session
   
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(urllib.parse.urljoin(url,'robots.txt'))
    rp.read()

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=3)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)




if __name__ == '__main__':

    set_start_method("spawn")
    
    
    start_time = time.time()


    ar = Archive('./data')
    
    file_name_zst = './'+DATASET_NAME+'.jsonl.zst'
    file_name_manifest = './'+DATASET_NAME+'.manifest'

    total_len = 0
    total_docs = 0
    total_sentences = 0
    total_words = 0
    total_verbs = 0
    total_nouns = 0
    total_punctuations = 0
    total_symbols = 0
    total_stopwords = 0
    total_oovs = 0


    total = 0
    added = 0


    # create and configure the process pool. All available resources are used by default
    


    with Pool(initializer=initialize_worker, initargs=[DATASET_URL], processes=PROCESSES) as pool:
        # issue tasks to the process pool
        for txt, meta in pool.imap(func=process_item, iterable=urls_generator(DATASET_URL), chunksize=1):
            total += 1
            if txt:
                total_words += meta['words']
                total_verbs += meta['verbs']
                total_nouns += meta['nouns']
                total_len += meta['length']
                total_docs += 1
                total_sentences += meta['sentences']
                total_punctuations += meta['punctuations']
                total_symbols += meta['symbols']
                total_stopwords += meta['stopwords']
                total_oovs += meta['oovs']
                ar.add_data(txt, meta = meta)
                added += 1
                print("Added "+str(total)+"/"+str(added)+" " + meta.get('url'))
            else:
                print("Skipping "+str(total)+"/"+str(added))


        # Close the process pool
        pool.close()
        # Wait for all tasks to complete
        pool.join()
    
 
    ar.commit()




    data_files= glob.glob('./data/*')
    file_size = 0

    #This solves an issue where data_files remains locked after ar commiting, causing error on cleanup
    ar = None

    for f in data_files:
        if f.endswith('.zst'):
            shutil.copy(f, os.path.join(file_name_zst))
            file_size = os.path.getsize(file_name_zst)

        os.remove(f)

    manifest = {"project" : "SpeakLeash", "name": DATASET_NAME, "description": DATASET_DESCRIPTION, "license": LICENSE, "category": DATASET_CATEGORY, "language": "pl", "file_size" : file_size, "sources": [{"name": DATASET_NAME, "url": DATASET_URL, "license": LICENSE}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols, "stopwords": total_stopwords, 'oovs': total_oovs}}
    json_manifest = json.dumps(manifest, indent = 4) 

    with open(file_name_manifest, 'w') as mf:
        mf.write(json_manifest)

    print(time.time()-start_time)