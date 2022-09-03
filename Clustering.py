# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 23:15:25 2022

@author: rcpat
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Connect and read from SQL DB
from sqlalchemy import create_engine
info = open(r"C:\Users\rcpat\Desktop\Personal Projects\Show22\psql_creds.txt",'r').read()
user = info.split(',')[0]
pw = info.split(',')[1]
conn_string = f"postgresql://localhost/personalprojects?user={user}&password={pw}"
engine = create_engine(conn_string)

players = pd.read_sql("select * from show22.card_stats", index_col = 'index', con = engine)

