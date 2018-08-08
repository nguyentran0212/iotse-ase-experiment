#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 16:38:57 2018

@author: nguyentran

This script sends [n] queries [q] to a search engine instance located at [addr]. Queries are [t] seconds
apart from each other.
"""

import requests
import time
import argparse
import json

def send_queries(n, addr, q, t):
    for i in range(n):
        query = None
        with open(q) as f:
            query = json.load(f)
        requests.post(addr, json = query)
        print("Sent query %s." % i)
        time.sleep(t)
    pass

if __name__ == "__main__":
    # Get parameters from the command line arguments
    parser = argparse.ArgumentParser(description="Tool for sending queries to an IoTSE instance")
    parser.add_argument("n", help="Number of queries to send.", type = int)
    parser.add_argument("q", help="Path to the JSON document specifying the query to send.")
    parser.add_argument("addr", help="Location of the IoTSE instance.")
    parser.add_argument("t", help="Delay between queries.", type = int)
    args = parser.parse_args()
    n = args.n
    q = args.q
    addr = args.addr
    t = args.t
    print("Parameters: %s %s %s %s" % (n, q, addr, t))
    
    # Send queries
    send_queries(n, addr, q, t)