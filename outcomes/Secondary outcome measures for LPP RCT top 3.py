#!/usr/bin/env python
# coding: utf-8

# # Secondary Outcomes
# **S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.**
# 
# **S2. Total items prescribed per 1000 registered patients for Co-proxamol.**
#  
# **S3. Total items prescribed per 1000 registered patients for Dosulepin.**

# In[1]:


import os
import requests
import pandas as pd
import numpy as np

from analysis import compute_regression

GBQ_PROJECT_ID = '620265099307'

# Set dates of baseline and follow-up periods
baseline_start = '2018-04-01'       # baseline start
mid_start = '2018-10-01'            # month after end of baseline period
followup_start = '2019-04-01'       # follow-up start
post_followup_start = '2019-10-01'  # month after end of follow-up period


# In[2]:


# Load data which should have been generated already by running the 
# primary outcome notebook
# (Specifically, per-measure cost/items numerators, and population denominators)
rawdata = pd.read_csv(os.path.join('..','data','all_measure_data.csv'))
rawdata["month"] = pd.to_datetime(rawdata.month)
rawdata.head(2)


# In[3]:


data = rawdata.copy()

### select data only for the baseline and follow-up periods
import datetime

conditions = [
    (data['month'] >= post_followup_start),
    (data['month'] >= followup_start),
    (data['month'] >= mid_start),
    (data['month'] >= baseline_start),
    (data['month'] < baseline_start)]

choices = ['after', 'follow-up', 'mid', 'baseline','before']
data['period'] = np.select(conditions, choices, default='0')

# take columns of interest from df
df2 = data[["measure","pct_id","period", "month", "cost","items","denominator"]]
df2 = df2.loc[(df2['period']== "baseline") | (df2['period']== "follow-up")].set_index(
    ["pct_id","period", "month"])
df2.head()


# In[4]:


### sum numerator and average population denominators for each CCG for each period
agg_6m = df2.groupby(["measure","pct_id","period"]).agg({"cost":sum,"items":sum,"denominator":"mean"})
agg_6m.head()

### CCGs that have been allocated in the RCT
ccgs = pd.read_csv(os.path.join('..','data','randomisation_group.csv'))
# import joint team information
team = pd.read_csv(os.path.join('..','data','joint_teams.csv'))

ccgs = ccgs.merge(team,on="joint_team", how="left")
#fill blank ccg_ids from joint_id column, so even CCGs not in Joint Teams 
# have a value for joint_id
ccgs["pct_id"] = ccgs["ccg_id"].combine_first(ccgs["joint_id"])
ccgs = ccgs[["joint_id","allocation","pct_id"]]
 
# Combine CCG/Joint Team info with measure data
rct_agg_6m = ccgs.merge(agg_6m.reset_index(), on="pct_id",how="left")
rct_agg_6m.head(3)


# In[5]:


# group up to Joint team groups 
# note: SUM both numerators and population denominator across geographies
rct_agg_6m = rct_agg_6m.groupby(["joint_id","allocation","measure","period"])       .sum().unstack().reset_index()
rct_agg_6m.columns = rct_agg_6m.columns.map('_'.join).map(lambda x: x.strip("_"))

### calculate aggregated measure values (cost only)
rct_agg_6m["baseline_calc_value"] = rct_agg_6m.cost_baseline / rct_agg_6m.denominator_baseline
rct_agg_6m["follow_up_calc_value"] = rct_agg_6m["cost_follow-up"] / rct_agg_6m["denominator_follow-up"]

rct_agg_6m.head(2)


# ## S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined. 

# In[6]:


# find top 3 measures per CCG by cost
top_3 = rct_agg_6m.sort_values(by=["joint_id","baseline_calc_value"], ascending=False)
top_3["measure_rank"] = top_3.groupby("joint_id")["baseline_calc_value"].rank(ascending=False)
top_3 = top_3.loc[top_3.measure_rank <=3]
top_3.head(2)


# In[7]:


# check whether any CCGs' top 3 include herbal medicine which was not available as a measure at the time of the intervention
top_3.loc[top_3["measure"]=="lpherbal"]


# In[10]:


top_3 = top_3.groupby(["joint_id","allocation"]).agg({"cost_baseline":"sum","cost_follow-up":"sum","denominator_baseline":"mean","denominator_follow-up":"mean"})

### calculate aggregated measure values for combined cost for the top 3 measures
top_3["baseline_calc_value"] = top_3.cost_baseline / top_3.denominator_baseline
top_3["follow_up_calc_value"] = top_3["cost_follow-up"] / top_3["denominator_follow-up"]
top_3.head(2) 


# In[12]:


# secondary outcome: Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.

data = top_3.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})

# summary data:
out = data.groupby("intervention").agg({"joint_id":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})
out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]
display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)


# ## S2: Total items prescribed per 1000 registered patients for Co-proxamol. 

# In[13]:


# filter data for coproxamol measure:
coprox = rct_agg_6m.copy()
coprox = coprox.loc[coprox.measure=="lpcoprox"]

### calculate aggregated measure values (items per 1000 patients)
coprox["baseline_calc_value"] = coprox.items_baseline / coprox.denominator_baseline
coprox["follow_up_calc_value"] = coprox["items_follow-up"] / coprox["denominator_follow-up"]
coprox.head(2)


# In[14]:


## Secondary outcome: Total items prescribed per 1000 registered patients for Co-proxamol.
data = coprox.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})

# summary data:
out = data.groupby("intervention").agg({"joint_id":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})
out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]
display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)


# ## S3: Total items prescribed per 1000 registered patients for Dosulepin. 

# In[15]:


# filter data for dosulepin measure:
dosulepin = rct_agg_6m.copy()
dosulepin = dosulepin.loc[dosulepin.measure=="lpdosulepin"]

### calculate aggregated measure values (items per 1000 patients)
dosulepin["baseline_calc_value"] = dosulepin.items_baseline / dosulepin.denominator_baseline
dosulepin["follow_up_calc_value"] = dosulepin["items_follow-up"] / dosulepin["denominator_follow-up"]
dosulepin.head(2)


# In[16]:


## Secondary outcome: Total items prescribed per 1000 registered patients for Dosulepin.
data = dosulepin.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})

# summary data:
out = data.groupby("intervention").agg({"joint_id":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})
out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]
display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)

