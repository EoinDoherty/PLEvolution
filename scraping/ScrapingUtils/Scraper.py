import requests
import queue
from bs4 import BeautifulSoup

DATA_FILENAME = "Data/metadata.csv"
INPUT_FILE = "Data/lang_links.csv"

def run_and_write():
    with open(INPUT_FILE) as f:
        raw_lines = f.readlines()
        
    processed_lines = [line.strip().split("\t")[1] for line in raw_lines]
    pages, _ = breadth_first(processed_lines)
    
    with open(DATA_FILENAME, "w") as f:
        for pid in pages:
            f.write(f"{pid}\t{pages[pid]}\n")

def get_pageid(page_name, S):
    QUERY_URL = "https://www.wikipedia.org/w/api.php/w/api.php?action=query&format=json&prop=&list=&meta=&indexpageids=1&titles="

    R = S.get(url=QUERY_URL + page_name)
    data = R.json()
    
    if 'query' in data and 'pageids' in data['query'] and len(data['query']['pageids']) > 0:
        return int(data['query']['pageids'][0])
    
    return 0

def get_info_from_pageid(pageid, session):
    response = session.get(url=f"https://en.wikipedia.org/w/api.php?action=parse&format=json&pageid={pageid}&prop=text&section=0&mobileformat=1&noimages=1")
    data = response.json()
    
    try:
        raw_html = data['parse']['text']['*']
        soup = BeautifulSoup(raw_html, "html.parser")
        processed = extract_metadata(soup)
        if processed["title"] == "":
            processed["title"] = data["parse"]["title"].lower()
        return processed
    except KeyError:
        print(f"No html text found for {pageid}, received {data}")
        return None

def extractLangs(row):
    ls = []

    for link in row.td.find_all('a'):
        ref = link.get("href")
        if ref[:6] == "/wiki/":
            ls.append(ref[6:])
    return ls
    
def extract_metadata(soup):
    paradigms = []
    influenced_by = []
    influenced = []
    typing = []
    year = 0
    title = ""

    ibox = soup.find("table", {"class": "infobox"})

    if ibox != None:
        if ibox.caption:
            title = ibox.caption.text.lower()

        rows = list(ibox.tbody.children)

        for i in range(len(rows)):
            row = rows[i]
            if row.th:
                if row.th.text.lower() == "typing discipline":
                    typing = [a.text.lower() for a in row.td.find_all('a')]
                if row.th.text.lower() == "first\xa0appeared":
                    year = row.td.contents[0]
                if row.th.text.lower() == "paradigm":
                    paradigms = [link.text for link in row.td.find_all('a')]
                if row.th.text.lower() == "influenced by":
                    influenced_by = extractLangs(rows[i+1])
                if row.th.text.lower() == "influenced":
                    influenced = extractLangs(rows[i+1])
    else:
        print("No infobox")

    return {"title": title, "year": year, "paradigm(s)": paradigms, "typing": typing, "influenced": influenced, "influenced by": influenced_by}

def breadth_first(ls):
    q = queue.Queue() # Queue of pageids to be processed
    url_mapping = {} # mapping from url ending to page id
    pages = {} # mapping of pageid to data for that page
    processing = set() # Set of all pageids that have been processed
    
    S = requests.Session()
    
    for url in ls:
        pid = get_pageid(url, S)
        
        if pid not in processing:
            q.put(pid)
            processing.add(pid)
            
        url_mapping[url] = pid
    
    while not q.empty():
        pageid = q.get()
        print(f"Processing {pageid}")
        
        # Only add page data if it's not already there
        if pageid not in pages:
            print(f"Getting data for {pageid}")
            page_data = get_info_from_pageid(pageid, S)
            influ_ids = []
            
            # Convert to pageids either using cache or api
            for url in page_data['influenced']:
                pid = 0
                
                if url in url_mapping:
                    pid = url_mapping[url]
                else:
                    pid = get_pageid(url, S)
                    if pid not in processing:
                        q.put(pid)
                        processing.add(pid)
                    url_mapping[url] = pid
                
                influ_ids.append(pid)
            page_data['influenced'] = influ_ids
            
            by_ids = []
            for url in page_data['influenced by']:
                pid = 0
                
                if url in url_mapping:
                    pid = url_mapping[url]
                else:
                    pid = get_pageid(url, S)
                    if pid not in processing:
                        q.put(pid)
                        processing.add(pid)
                    url_mapping[url] = pid
                
                by_ids.append(pid)
            page_data['influenced by'] = by_ids
            
            pages[pageid] = page_data
    
    print("Done")
    return pages, url_mapping
