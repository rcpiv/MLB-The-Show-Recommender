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
## Use Shohei Live Series as test since he has everything

shohei = items_df[items_df['uuid'] == 'b4857835b5b69aef8e214f5e0094126a']

### Drop unneeded columns
columns2drop = ['type','series_texture_name','is_sellable','has_augment',\
                'augment_text','augment_end_date','has_matchup','stars',\
                    'trend','new_rank','has_rank_change','event','pitches','quirks']
quirks_img = list(shohei.filter(regex='quirks_(.*)_img'))
quirks_desc = list(shohei.filter(regex='quirks_(.*)_description'))

columns2drop = columns2drop + quirks_img + quirks_desc

shohei = shohei.drop(columns2drop, axis = 1)

### Convert height and weight to numeric
def get_inches(h):
    l = re.split("\'", h)
    height = int(l[0])*12 + int(l[1][:-1])
    return height

shohei['height'] = shohei['height'].apply(get_inches)




get_inches('6\'4"')


