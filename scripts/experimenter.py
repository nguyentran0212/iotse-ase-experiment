#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 19:28:45 2018

@author: nguyentran

Experimenter is a software system for conducting experiments on IoTSE instances
It is responsible for sending queries to an IoTSE instance, and extracting data
from the conductor engine to produce visualisation on response time 
and overhead of an IoTSE instance

It reads a JSON description and conduct the experiment accordingly
"""

import requests
import time
import argparse
import json
from bs4 import BeautifulSoup
import pandas as pd
import pickle
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
import time

class Experiment:
    def __init__(self, architecture_pattern_id):
        self.architecture_pattern_id = architecture_pattern_id
        self.workflows = None
    
    def set_workflows(self, workflows):
        self.workflows = workflows
    

class Workflow:
    def __init__(self, workflow_id, time_start, time_end, time_elapsed, is_parallel = False):
        self.workflow_id = workflow_id
        self.time_start = time_start
        self.time_end = time_end
        self.time_elapsed = time_elapsed
        self.overhead = None
        self.activities = None
        self.is_parallel = is_parallel
        
    def set_activities(self, activities):
        self.activities = activities

    def determine_overhead(self, assign = True):
        total_execution_time = 0
        overhead = 0
        
        if self.is_parallel == True:
            # Calculate the time between forking and joining
            time_fork = 0
            time_join = 0
            for activity in self.activities:
                if activity.task_name == "forkx":
                    time_fork = activity.time_start
                if activity.task_name == "join":
                    time_join = activity.time_end
            time_parallel = time_join - time_fork
            
            # Calculate the time of remaining tasks
            for activity in self.activities:
                if activity.time_start < time_fork or activity.time_start > time_join:
                    total_execution_time += activity.time_elapsed
            
            # Calculate total execution time
            total_execution_time += time_parallel
        else:
            for activity in self.activities:
                total_execution_time += activity.time_elapsed
        
        overhead = self.time_elapsed - total_execution_time
        
        if assign:
            self.overhead = overhead
        
        return overhead


class Activity:
    def __init__(self, task_name, time_start, time_end):
        self.task_name = task_name
        self.time_start = time_start
        self.time_end = time_end
        self.time_elapsed = self.time_end - self.time_start


class Visualiser:
    def draw_boxplot_response_time(self, con_df):
        fig, ax = plt.subplots()
        fig.set_size_inches(12, 5)
        g = sns.boxplot(data = con_df.sort_values(by=["architecture_pattern_id"]), x = "architecture_pattern_id", y = "time_elapsed", ax = ax)
        plt.title("Response time of different architectural patterns")
        plt.xlabel("Architectural Pattern")
        plt.ylabel("Response Time in ms")
        plt.tight_layout()
        plt.ylim(0)
        plt.savefig("response_time.png", dpi = 300)
        plt.show(g)
        return g
    
    def draw_boxplot_overhead(self, con_df):
        fig, ax = plt.subplots()
        fig.set_size_inches(12, 5)
        g = sns.boxplot(data = con_df.sort_values(by=["architecture_pattern_id"]), x = "architecture_pattern_id", y = "overhead", ax = ax)
        plt.title("Overhead of different architectural patterns")
        plt.xlabel("Architectural Pattern")
        plt.ylabel("Overhead in ms")
        plt.tight_layout()
        plt.ylim(0)
        plt.savefig("overhead.png", dpi = 300)
        plt.show(g)
        return g
    
    def draw_boxplot_overhead_percentage(self, con_df):
        con_df["overhead_percentage"] = con_df.apply(lambda row : row["overhead"] * 100.0 / row["time_elapsed"], axis = 1)
        fig, ax = plt.subplots()
        fig.set_size_inches(12, 5)
        g = sns.boxplot(data = con_df.sort_values(by=["architecture_pattern_id"]), x = "architecture_pattern_id", y = "overhead_percentage", ax = ax)
        plt.title("Percentage of overhead of different architectural patterns")
        plt.xlabel("Architectural Pattern")
        plt.ylabel("Percentage")
        plt.tight_layout()
        plt.ylim(0)
        plt.savefig("overhead_percentage.png", dpi = 300)
        plt.show(g)
        return g
    

class Analyser:
    pass

class Scraper:
    def extract_workflows(self, addr_conductor, skip_workflow_types):
        def extract_from_payload(payload, i, key, skip_error = True):
            extract = payload["result"]["hits"][i][key]
            return extract
        
        conductor_url = addr_conductor + "api/wfe/?q=&h=&freeText=&start=0"
        wfs = requests.get(conductor_url).json()
        n = len(wfs["result"]["hits"])
        data = {"workflowType" : [],
                "workflowId" : [],
                "startTime" : [],
                "endTime" : [],
                "executionTime" : []
                }
        
        #print(len([wfs["result"]["hits"][x]["workflowId"] for x in range(96) if wfs["result"]["hits"][x]["workflowType"] == 'iotse_prototype_search_para']))
        for i in range(n):
            for key in data.keys():
                if key == "workflowType" and extract_from_payload(wfs, i, key) in skip_workflow_types:
                    break
                try:
                    data[key].append(extract_from_payload(wfs, i, key))
                except KeyError:
                    data[key].append(None)
                
        df = pd.DataFrame(data, columns = data.keys())
        return df
    
    def extract_workflow_activities(self, addr_conductor, workflow_id):
        def extract_from_payload(payload, i, key, skip_error = True):
            extract = payload["result"]["tasks"][i][key]
            return extract
        
        workflow_url = "%sapi/wfe/id/%s" % (addr_conductor, workflow_id)
        workflow_details = requests.get(workflow_url).json()
        n = len(workflow_details["result"]["tasks"])
        data = {"taskType" : [],
                "referenceTaskName" : [],
                "retryCount" : [],
                "startTime" : [],
                "endTime" : [],
                }
        
        for i in range(n):
            for key in data.keys():
                try:
                    data[key].append(extract_from_payload(workflow_details, i, key))
                except KeyError:
                    data[key].append(None)
                
        df = pd.DataFrame(data, columns = data.keys())
        return df


class Experimenter:
    def __init__(self, n, addr, q, t, sleep, addr_conductor, architecture_pattern_id, is_parallel, experiment_folder):
        self.n = n
        self.addr = addr
        self.q = q
        self.t = t
        self.sleep = sleep
        self.addr_conductor = addr_conductor
        self.architecture_pattern_id = architecture_pattern_id
        self.is_parallel = is_parallel
        self.experiment_folder = experiment_folder
    
    def start_experiment(self, query = False, measure = False, analyse = False):
        if query:
            self.send_queries(self.n, self.addr, self.q, self.t)
            time.sleep(self.sleep)
        if measure:
            self.measure(self.addr_conductor, self.architecture_pattern_id)
        if analyse:
            self.analyse(self.experiment_folder)
        
    
    def send_queries(self, n, addr, q, t):
        print("================\nStart sending queries.")
        for i in range(n):
            query = None
            with open(q) as f:
                query = json.load(f)
            requests.post(addr, json = query)
            print("Sent query %s." % (i+1))
            time.sleep(t)
        print("Finish sending queries.\n================\n")
    
    def measure(self, addr_conductor, architecture_pattern_id, skip_workflow_types = ("iotse_prototype_discovery", "kitchensink")):        
        print("================\nStart extracting performance data.")
        
        scraper = Scraper()
        # Extract the list of workflows into a dataframe
        # And then construct a list of Workflow objects from the dataframe
        wf_df = scraper.extract_workflows(addr_conductor, skip_workflow_types = skip_workflow_types)
        workflows = list(wf_df.apply(lambda row : Workflow(row["workflowId"], row["startTime"], row["endTime"], row["executionTime"], is_parallel = self.is_parallel), axis = 1))
        
        for workflow in workflows:
            # Extract the list of activities of the workflow into a dataframe
            # And then construct a list of Activity objects from the dataframe
            activities_df = scraper.extract_workflow_activities(addr_conductor, workflow.workflow_id)
            activities = list(activities_df.apply(lambda row : Activity(row["referenceTaskName"], row["startTime"], row["endTime"]), axis = 1))
            workflow.set_activities(activities)
            workflow.determine_overhead()

        experiment = Experiment(architecture_pattern_id)
        experiment.set_workflows(workflows)

        pickle.dump(experiment, open("%s/%s.p" % (self.experiment_folder, architecture_pattern_id), "wb"))
        print("Finish extracting performance data.\n================\n")        
        
        
    def analyse(self, experiment_folder):
        def load_dfs(root_folder):
            root_p = Path(root_folder)
            dfs = {}
            con_df = pd.DataFrame()
            
            for file_p in root_p.iterdir():
                print("================\nProcessing '%s'." % file_p)
                if not file_p.is_file() :
                    print("This is not a file. Skipped.")
                experiment = None
                try:
                    experiment = pickle.load(open(file_p, "rb"))
                except (pickle.UnpicklingError, IsADirectoryError) as e:
                    continue
                
                # Generate a dataframe from the experiment object
                n = len(experiment.workflows)
                data = {"workflow_id" : [],
                        "time_start" : [],
                        "time_end" : [],
                        "time_elapsed" : [],
                        "overhead" : []
                        }
                
                for i in range(n):
                    for key in data.keys():
                        try:
                            data[key].append(getattr(experiment.workflows[i], key))
                        except KeyError:
                            data[key].append(None)
                        
                df = pd.DataFrame(data, columns = data.keys())
                
                df_name = file_p.parts[-1].split(".")[0]
                ls = [df_name] * n
                df["architecture_pattern_id"] = ls
                dfs[df_name] = df
                print("Loaded dataframe:", df.info())
                con_df = con_df.append(df)
            print("================\nFinished loading %d dataframes.\n\n\n" % len(list(dfs.keys())))
            return dfs, con_df
        
        print("================\nStart visualising.")
        visualiser = Visualiser()
        dfs, con_df = load_dfs(experiment_folder)
        visualiser.draw_boxplot_response_time(con_df)
        visualiser.draw_boxplot_overhead(con_df)
        visualiser.draw_boxplot_overhead_percentage(con_df)
        print("Finish visualising.\n================\n")
        

if __name__ == "__main__":
    n = 3
    addr = "http://localhost:5000/queries"
    q = "/home/nguyentran/CodeProjects/IoTSE/IoTSE_experiments/sample_query_multi.json"
    t = 5
    sleep = 30
    addr_conductor = "http://localhost:5050/"
    architecture_pattern_id = "multi_ID_PS_D"
    is_parallel = True
    experiment_folder = "/home/nguyentran/CodeProjects/IoTSE/IoTSE_experiments/"
    
    experimenter = Experimenter(n, addr, q, t, sleep, addr_conductor, architecture_pattern_id, is_parallel, experiment_folder)
#    experimenter.start_experiment(query = True)
#    experimenter.start_experiment(measure = True)
    experimenter.start_experiment(analyse = True)
#    experimenter.start_experiment(query = True, measure = True)
#    experimenter.measure("http://localhost:5050/", "ID-SS_C")
#    experimenter.analyse("/home/nguyentran/CodeProjects/IoTSE/IoTSE_experiments/CaseStudy3/scripts/")