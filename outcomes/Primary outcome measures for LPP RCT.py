#!/usr/bin/env python
# coding: utf-8

# # Primary outcomes
# **P1.  Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined.**
# 
# **P2. Total items per 1000 across all 18 low priority treatments.**
# 

# In[1]:


import os
import requests
import pandas as pd
import numpy as np

from analysis import compute_regression

import logging
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.ERROR)

GBQ_PROJECT_ID = '620265099307'
DUMMY_RUN = False  # Useful for testing; set to false when doing real analysis

# Set dates of baseline and follow-up periods
baseline_start = '2018-04-01'       # baseline start
mid_start = '2018-10-01'            # month after end of baseline period
followup_start = '2019-04-01'       # follow-up start
post_followup_start = '2019-10-01'  # month after end of follow-up period

all_measures = ['lpcoprox', 'lpdosulepin', 'lpdoxazosin', 
                'lpfentanylir', 'lpglucosamine', 'lphomeopathy', 
                'lplidocaine', 'lpliothyronine', 'lplutein', 
                'lpomega3', 'lpoxycodone', 'lpperindopril', 
                'lprubefacients', 'lptadalafil', 'lptramadolpara', 
                'lptravelvacs', 'lptrimipramine','lpherbal']
definition_url = (
    "https://raw.githubusercontent.com/ebmdatalab/openprescribing/"
    "{commit}/openprescribing/frontend/management/commands/measure_definitions/"
    "{measure}.json")
commit_for_measure_definitions = "6f949660fee06401102136926eaba075d963511d"

# import herbal list manually due to different construction of query based on a separate file
herbal_bnf_list = pd.read_csv(os.path.join('..','data','herbal_list.csv'), usecols=["bnf_code"])
herbal_bnf_list = tuple(herbal_bnf_list["bnf_code"])


# In[2]:


# Import data from BigQuery
# (Specifically, per-measure cost/items numerators, and population denominators)
if DUMMY_RUN and os.path.exists(os.path.join('..','data','all_measure_data.csv')):
    rawdata = pd.read_csv(os.path.join('..','data','all_measure_data.csv')).drop(['Unnamed: 0'], axis=1)
else:
    rawdata = pd.DataFrame()
    sql_template = open("measure.sql", "r").read()
    for measure in all_measures:
        if measure == "lpherbal":
            where_condition = f"(bnf_code IN {herbal_bnf_list})"
        else:
            measure_definition = requests.get(definition_url.format(
                commit=commit_for_measure_definitions, measure=measure)).json()
            where_condition = " ".join(measure_definition['numerator_where'])
        sql = sql_template.format(
            date_from=baseline_start, 
            date_to=post_followup_start, 
            where_condition=where_condition)
        df = pd.read_gbq(sql, GBQ_PROJECT_ID, dialect='standard')
        df["month"] = pd.to_datetime(df.month)
        df["measure"] = measure
        rawdata = rawdata.append(df)
rawdata.head(1)


# In[3]:


rawdata.to_csv(os.path.join('..','data','all_measure_data.csv'))


# In[4]:


# Aggregate across all measures 
data = rawdata.groupby(["pct_id", "month"]).agg(
    {'items':'sum', 'cost': 'sum', 'denominator':'first'}).reset_index()
data = data.rename(columns={"cost": "numerator"})
data['calc_value'] = data['numerator'] / data['denominator']
data.head(2)


# In[5]:


# select data only for the baseline and follow-up periods

conditions = [
    (data['month'] >= post_followup_start),
    (data['month'] >= followup_start),
    (data['month'] >= mid_start),
    (data['month'] >= baseline_start),
    (data['month'] < baseline_start)]

choices = ['after', 'follow-up', 'mid', 'baseline', 'before']
data['period'] = np.select(conditions, choices, default='0')
# Restrict to columns of interest
data = data[["pct_id", "period", "month", "numerator", "denominator", "items"]]
data = data.loc[
    (data['period'] == "baseline") | (data['period'] == "follow-up")
].set_index(["pct_id", "period", "month"])

data.head(3)


# In[6]:


# group measurements for each CCG for each period
agg_6m = data.groupby(["pct_id", "period"]).agg(
    {"numerator": "sum", "items": "sum", "denominator": "mean"})
agg_6m.head()


# In[7]:


### CCGs that have been allocated to the RCT 
rct_ccgs = pd.read_csv(os.path.join('..','data','randomisation_group.csv'))


# Joint Team information (which CCGs work together in Joint Teams)
team = pd.read_csv(os.path.join('..','data','joint_teams.csv'))

# Map CCGs to Joint Teams
rct_ccgs = rct_ccgs.merge(team, on="joint_team", how="left")

# Fill blank ccg_ids from joint_id column, so even CCGs not in Joint Teams 
# have a value for joint_id
rct_ccgs["pct_id"] = rct_ccgs["ccg_id"].combine_first(rct_ccgs["joint_id"])
rct_ccgs = rct_ccgs[["joint_id", "allocation", "pct_id"]]

# Combine CCG/Joint Team info with measure data
rct_ccgs = rct_ccgs.merge(agg_6m.reset_index(), on="pct_id", how="left")
rct_ccgs.head(3)


# In[8]:


# aggregate up to Joint team groups
# sum both numerator and population denominator across joint teams 
rct_agg_6m = rct_ccgs             .groupby(["joint_id", "allocation", "period"])             .sum()             .unstack()             .reset_index()
# Rename columns which have awkward names resulting from the unstack operation
rct_agg_6m.columns = rct_agg_6m.columns.map('_'.join).map(lambda x: x.strip("_"))
# Create binary "intervention" column for later regression
rct_agg_6m['intervention'] = rct_agg_6m.allocation.map({'con': 0, 'I': 1})
rct_agg_6m.head(3)


# In[9]:


# calculate aggregated measure values for baseline and followup pareiods
rct_agg_6m["baseline_calc_value"] = (
    rct_agg_6m.numerator_baseline / rct_agg_6m.denominator_baseline)
rct_agg_6m["follow_up_calc_value"] = (
    rct_agg_6m["numerator_follow-up"] / rct_agg_6m["denominator_follow-up"])
rct_agg_6m["baseline_items_thou"] = (
    rct_agg_6m.items_baseline / rct_agg_6m.denominator_baseline)
rct_agg_6m["follow_up_items_thou"] = (
    rct_agg_6m["items_follow-up"] / rct_agg_6m["denominator_follow-up"])

rct_agg_6m.head(3)


# 
# # Baseline characteristics

# In[10]:



# Obtain list sizes including older-age proportion

sql = '''SELECT pct_id, AVG(over_65)/1000 AS over_65, AVG(total_list_size) AS total_list_size --average over 6-month period
FROM (SELECT prac.ccg_id AS pct_id, month,
SUM(male_65_74+ male_75_plus+ female_65_74+ female_75_plus) AS over_65, -- sum up to CCGs
SUM(total_list_size) AS total_list_size
FROM ebmdatalab.hscic.practice_statistics_all_years stats
LEFT JOIN ebmdatalab.research.practices_2019_09 prac ON stats.practice = prac.code
WHERE month between '{date_from}' and '{date_to}'
GROUP BY pct_id, month)
GROUP BY pct_id'''

sql = sql.format(date_from=baseline_start, date_to=mid_start)

#sql = open("measure.sql", "r").read()
pop = pd.read_gbq(sql, GBQ_PROJECT_ID, dialect='standard')
pop.to_csv(os.path.join('..','data','ccg_populations.csv'))
pop.head()


# In[11]:


baselines = rct_ccgs.merge(pop, on="pct_id").loc[rct_ccgs["period"]=="baseline"]

baselines = baselines.groupby(["joint_id","allocation"]).sum().reset_index()
baselines["calc_value"] = baselines["numerator"]/baselines["denominator"]
baselines["items_thou"] = baselines["items"]/baselines["denominator"]
baselines["percent_over_65"] = 100*baselines["over_65"]/baselines["denominator"]

# group over control and intervention groups:
baselines.groupby("allocation").agg({
    "joint_id":"count",
    "percent_over_65":["mean","std"],
    "denominator":["mean","std"],
    "items_thou":["mean","std"],
    "calc_value":["mean","std"]
    }).round(1)


# # Primary Outcome P1
# 
# Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined, between intervention and control groups, assessed by applying a multivariable linear regression model.
# 

# In[12]:


# summary data
out = rct_agg_6m.groupby("allocation").agg({"joint_id":"nunique",
                                                "baseline_calc_value":{"mean","std"},
                                                "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out.sort_index(ascending=False))

# regression analysis
formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(rct_agg_6m, formula=formula)


# # Primary Outcome P2 
# ITEMS per 1,000 patients for all 18 pre-specified “low-priority” treatments combined, between intervention and control groups, assessed by applying a multivariable linear regression model.
# 

# In[13]:


# summary data
out = rct_agg_6m.groupby("allocation").agg({"joint_id":"nunique",
                                                "baseline_items_thou":{"mean","std"},
                                                "follow_up_items_thou":{"mean","std"}})

out["change"] = out[("follow_up_items_thou","mean")] - out[("baseline_items_thou","mean")]

display(out.sort_index(ascending=False))

# regression analysis
formula = ('data["follow_up_items_thou"] '
           '~ data["baseline_items_thou"] + intervention')
compute_regression(rct_agg_6m, formula=formula)


# ### Sensitivity Analysis

# Some CCGs did not successfully receive the intervention.

# In[14]:


visit = pd.read_csv(os.path.join('..','data','allocated_ccgs_visit_timetable.csv'))
visit["flag"] = np.where(visit["date"].str.len()>0,1,0)

data2 = rct_agg_6m.merge(visit, on="joint_id", how="left").drop("date", axis=1)
data2["flag"] = data2["flag"].fillna(0).astype("int")

# summary data
out = data2.groupby("flag").agg({"joint_id":"nunique",
                                 "baseline_calc_value":{"mean","std"},
                                 "follow_up_calc_value":{"mean","std"}})

out["change"] = out[("follow_up_calc_value","mean")] - out[("baseline_calc_value","mean")]

display(out)

# regression analysis
formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + flag')
compute_regression(data2, formula=formula)


# In[15]:


# summary data
out = data2.groupby("flag").agg({"joint_id":"nunique",
                                 "baseline_items_thou":{"mean","std"},
                                 "follow_up_items_thou":{"mean","std"}})

out["change"] = out[("follow_up_items_thou","mean")] - out[("baseline_items_thou","mean")]

display(out)

# regression analysis
formula = ('data["follow_up_items_thou"] '
           '~ data["baseline_items_thou"] + flag')
compute_regression(data2, formula=formula)

