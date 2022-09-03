# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 21:16:06 2022

@author: rcpat
"""
#%% Import packages
import pandas as pd
import numpy as np
import time
import json
import requests
import datetime as dt
import concurrent.futures
import requests
import time
import ast
from flatten_json import *
import re

#%% Initialize variables and data structures
types_pages = {}
out = []
CONNECTIONS = 1000
TIMEOUT = 5

# Get number of API pages
url = requests.get("https://mlb22.theshow.com/apis/items.json?type=mlb_card")
text = url.text
data = json.loads(text)
n_pages = list(range(1,data['total_pages']+1))

# Generate all URLS
urls = []
for n in n_pages:
    urls.append(f"https://mlb22.theshow.com/apis/items.json?type=mlb_card&page={n}")

# Define load_url function
def load_url(url, timeout):
    ans = requests.get(url, timeout=timeout)
    return ans.text

# Make API calls
with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, url, TIMEOUT) for url in urls)
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            data = future.result()
            data = json.loads(data)['items']
        except Exception as exc:
            data = str(type(exc))
        finally:    
            out.append(data)

# Convert API reads into dataframe
import itertools
n = 0
for i in out:
    if n == 0:
        flat = [flatten(d) for d in i]
        items_df = pd.DataFrame(flat)
    else:
        flat = [flatten(d) for d in i]
        items_df = pd.concat([items_df,pd.DataFrame(flat)])
    n += 1
    
# Data Cleaning
### Drop unneeded columns
columns2drop = ['type','series_texture_name','is_sellable','has_augment',\
                'augment_text','augment_end_date','has_matchup','stars',\
                    'trend','new_rank','has_rank_change','event','pitches','quirks']
quirks_img = list(items_df.filter(regex='quirks_(.*)_img'))
quirks_desc = list(items_df.filter(regex='quirks_(.*)_description'))

columns2drop = columns2drop + quirks_img + quirks_desc

items_df = items_df.drop(columns2drop, axis = 1)

### Convert height and weight to numeric
def get_inches(h):
    l = re.split("\'", h)
    height = int(l[0])*12 + int(l[1][:-1])
    return height

items_df['height'] = items_df['height'].apply(get_inches)
items_df['weight'] = items_df['weight'].str.split(' ').str[0].astype('int')

### Hit and Field Rank
def multi_split(s):
    """
    s = string
    """
    delimiters = "/",".","-"
    regex_pattern = '|'.join(map(re.escape, delimiters))
    l = re.split(regex_pattern, s)
    try:
        return l[-2].title()
    except:
        pass

items_df['hit_rank'] = items_df['hit_rank_image'].apply(multi_split)
items_df['field_rank'] = items_df['fielding_rank_image'].apply(multi_split)

items_df = items_df.drop(['hit_rank_image','fielding_rank_image'], axis = 1)

# Push to SQL
from sqlalchemy import create_engine
info = open(r"C:\Users\rcpat\Desktop\Personal Projects\Show22\psql_creds.txt",'r').read()
user = info.split(',')[0]
pw = info.split(',')[1]
conn_string = f"postgresql://localhost/personalprojects?user={user}&password={pw}"
engine = create_engine(conn_string)
items_df.to_sql('card_stats',con=engine, if_exists = 'replace', schema = 'show22')
