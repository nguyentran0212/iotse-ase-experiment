#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 13:38:50 2018

@author: nguyentran

This script visualises response time of different types of IoTSE as boxplot
and heatmap. It accepts a folder of csv files, storing the workflow execution time
of different workflow patterns, and generate boxplot from those files.
"""
from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

def draw_boxplot(con_df):
    g = sns.boxplot(data = con_df, x = "or_pat", y = "executionTime")
    plt.title("Response time of different Orchestration patterns")
    plt.xlabel("Orchestration Pattern")
    plt.ylabel("Response Time in ms")
    plt.tight_layout()
#    plt.show(g)
    return g
    
if __name__ == "__main__":    
    root_folder = "/home/nguyentran/CodeProjects/IoTSE/IoTSE_experiments/data/raw"
    dfs, con_df = load_dfs(root_folder)
        
    con_df = con_df.drop(["Unnamed: 0", "workflowType", "workflowId"], axis = 1)
    print(con_df.info())
    g = draw_boxplot(con_df)
    plt.savefig("res_time_4_pat.png", dpi = 300)
    