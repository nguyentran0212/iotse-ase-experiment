#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 14:38:03 2018

@author: nguyentran

This script calculates statistical significance of difference in response time
between orchestration patterns
"""

from pathlib import Path
import pandas as pd
from pandas.tools.plotting import table
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

# Load css files from named folders in the root folder, and store them in dictionary
# Data frame are named by the name of the CSV file
def load_dfs(root_folder):
    root_p = Path(root_folder)
    dfs = {}
    con_df = pd.DataFrame()
    
    for sub_p in root_p.iterdir():
        for file_p in sub_p.iterdir():
            print("================\nProcessing '%s'." % file_p)
            if not file_p.is_file():
                print("This is not a file. Skipped.")
            df = pd.read_csv(file_p)
            df_name = file_p.parts[-1].split(".")[0]
            df_n_row = df.shape[0]
            ls = [df_name] * df_n_row
            df["or_pat"] = ls
            dfs[df_name] = df
            print("Loaded dataframe:", df.info())
            con_df = con_df.append(df)
    print("================\nFinished loading %d dataframes.\n\n\n" % len(list(dfs.keys())))
    return dfs, con_df

def cal_t_tests(dfs):
    df_t_tests = pd.DataFrame(columns = ["Pat_1", "Pat_2", "t", "p"])
    for i in range(len(list(dfs.keys()))):
        pat_1 = list(dfs.keys())[i]
        df_1 = dfs[pat_1]
        for j in range(len(list(dfs.keys()))):
            pat_2 = list(dfs.keys())[j]
            df_2 = dfs[pat_2]
            t, p = stats.ttest_ind(df_1["executionTime"], df_2["executionTime"])
            df_t_tests = df_t_tests.append({"Pat_1" : pat_1, "Pat_2" : pat_2, "t" : t, "p" : p}, ignore_index = True)
    return df_t_tests
    
if __name__ == "__main__":    
    root_folder = "/home/nguyentran/CodeProjects/IoTSE/IoTSE_experiments/data/raw"
    dfs, con_df = load_dfs(root_folder)
        
    df_t_tests = cal_t_tests(dfs)
    pivoted_df_p = df_t_tests.pivot(index = "Pat_1", columns = "Pat_2", values = "p")
    pivoted_df_t = df_t_tests.pivot(index = "Pat_1", columns = "Pat_2", values = "t")
    print(pivoted_df_p)
    print(pivoted_df_t)
    df_t_tests.to_csv("t_tests_4_pat.csv")
    pivoted_df_p.to_csv("t_tests_4_pat_pivoted_p.csv")
    pivoted_df_t.to_csv("t_tests_4_pat_pivoted_t.csv")
    