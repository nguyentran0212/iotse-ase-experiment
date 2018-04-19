#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 17:18:23 2018

@author: nguyentran
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_from_payload(payload, i, key, skip_error = True):
    extract = payload["result"]["hits"][i][key]
    return extract

conductor_url = "http://localhost:5050/api/wfe/?q=&h=&freeText=&start=0"
wfs = requests.get(conductor_url).json()
n = len(wfs["result"]["hits"])
data = {"workflowType" : [],
        "workflowId" : [],
        "startTime" : [],
        "endTime" : [],
        "executionTime" : []
        }
out_path = "PD-SS_C.csv"
skip_workflow_type = "iotse_prototype_discovery"

#print(len([wfs["result"]["hits"][x]["workflowId"] for x in range(96) if wfs["result"]["hits"][x]["workflowType"] == 'iotse_prototype_search_para']))
for i in range(n):
        for key in data.keys():
            print(i, n)
            if key == "workflowType" and extract_from_payload(wfs, i, key) == skip_workflow_type:
                break
            try:
                data[key].append(extract_from_payload(wfs, i, key))
            except KeyError:
                data[key].append(None)
        
    
df = pd.DataFrame(data, columns = data.keys())
print(df)
df.to_csv(out_path)