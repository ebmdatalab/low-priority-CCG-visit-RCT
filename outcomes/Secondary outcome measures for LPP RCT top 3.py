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


rawdata = pd.read_csv(os.path.join('..','data','all_measure_data.csv'))
rawdata["month"] = pd.to_datetime(rawdata.month)
rawdata.head()


# ## S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined. 

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
df2 = df2.loc[(df2['period']== "baseline") | (df2['period']== "follow-up")].set_index(["pct_id","period", "month"])
df2.head()


# In[4]:


### sum numerator and average population denominators for each CCG for each period
agg_6m = df2.groupby(["measure","pct_id","period"]).agg({"cost":sum,"items":sum,"denominator":"mean"})
agg_6m.head()


# In[5]:


### import **allocated** CCGs
ccgs = pd.read_csv(os.path.join('..','data','randomisation_group.csv'))
# import joint team information
team = pd.read_csv(os.path.join('..','data','joint_teams.csv'))

ccgs = ccgs.merge(team,on="joint_team", how="left")
#fill black ccg_ids from joint_id column
ccgs["pct_id"] = ccgs["ccg_id"].combine_first(ccgs["joint_id"])
ccgs = ccgs[["joint_id","allocation","pct_id"]]

df2b = agg_6m.reset_index()
df2b = ccgs.merge(df2b, on="pct_id",how="left")
df2b.head()


# In[6]:


# group up to Joint team groups 
# note: SUM both numerators and population denominator across geographies
df2c = df2b.groupby(["joint_id","allocation","measure","period"]).sum()
df2c = df2c.unstack().reset_index()
df2c.columns = df2c.columns.map('_'.join)

### calculate aggregated measure values (cost only)
df2c["baseline_calc_value"] = df2c.cost_baseline / df2c.denominator_baseline
df2c["follow_up_calc_value"] = df2c["cost_follow-up"] / df2c["denominator_follow-up"]

df2c.head()


# In[7]:


# find top 3 measures per CCG by cost
df3 = df2c.sort_values(by=["joint_id_","baseline_calc_value"], ascending=False)
df3["measure_rank"] = df3.groupby("joint_id_")["baseline_calc_value"].rank(ascending=False)
df4 = df3.loc[df3.measure_rank <=3]
df4.head()


# In[8]:


df5 = df4.copy()
df5 = df5.groupby(["joint_id_","allocation_"]).agg({"cost_baseline":"sum","cost_follow-up":"sum","denominator_baseline":"mean","denominator_follow-up":"mean"})

### calculate aggregated measure values for combined cost for the top 3 measures
df5["baseline_calc_value"] = df5.cost_baseline / df5.denominator_baseline
df5["follow_up_calc_value"] = df5["cost_follow-up"] / df5["denominator_follow-up"]
df5.head() 


# In[9]:


# secondary outcome: Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.

import statsmodels.formula.api as smf
data = df5.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

out = data.groupby("intervention").agg({"joint_id_":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)


# ## S2: Total items prescribed per 1000 registered patients for Co-proxamol. 

# In[10]:


# filter data for coproxamol measure:
df6 = df2c.copy()
df6 = df6.loc[df6.measure_=="lpcoprox"]

### calculate aggregated measure values (items per 1000 patients)
df6["baseline_calc_value"] = df6.items_baseline / df6.denominator_baseline
df6["follow_up_calc_value"] = df6["items_follow-up"] / df6["denominator_follow-up"]
df6.head()


# In[11]:


## Secondary outcome: Total items prescribed per 1000 registered patients for Co-proxamol.
import statsmodels.formula.api as smf
data = df6.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})


out = data.groupby("intervention").agg({"joint_id_":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)


# ## S3: Total items prescribed per 1000 registered patients for Dosulepin. 

# In[12]:


# filter data for dosulepin measure:
df7 = df2c.copy()
df7 = df7.loc[df7.measure_=="lpdosulepin"]

### calculate aggregated measure values (items per 1000 patients)
df7["baseline_calc_value"] = df7.items_baseline / df7.denominator_baseline
df7["follow_up_calc_value"] = df7["items_follow-up"] / df7["denominator_follow-up"]
df7.head()


# In[13]:


## Secondary outcome: Total items prescribed per 1000 registered patients for Dosulepin.
import statsmodels.formula.api as smf
data = df7.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

out = data.groupby("intervention").agg({"joint_id_":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)


# # Additional analyses (not pre-specified)

# **1. Change in top 3 measures per CCG by cost - *excluding herbal* which was not included at the time of the interventions.**

# In[14]:


# find top 3 measures per CCG by cost - excluding herbal which was not included at the time of the interventions.
df3 = df2c.loc[df2c["measure_"] != "lpherbal"]
df3 = df3.sort_values(by=["joint_id_","baseline_calc_value"], ascending=False)
df3["measure_rank"] = df3.groupby("joint_id_")["baseline_calc_value"].rank(ascending=False)
df4 = df3.loc[df3.measure_rank <=3]
df4.head()


# In[15]:


df5 = df4.copy()
df5 = df5.groupby(["joint_id_","allocation_"]).agg({"cost_baseline":"sum","cost_follow-up":"sum","denominator_baseline":"mean","denominator_follow-up":"mean"})

### calculate aggregated measure values for combined cost for the top 3 measures
df5["baseline_calc_value"] = df5.cost_baseline / df5.denominator_baseline
df5["follow_up_calc_value"] = df5["cost_follow-up"] / df5["denominator_follow-up"]
df5.head() 


# In[16]:


# secondary outcome: Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.

import statsmodels.formula.api as smf
data = df5.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

out = data.groupby("intervention").agg({"joint_id_":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out)

formula = ('data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention')
compute_regression(data, formula=formula)

