# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.4
#   kernelspec:
#     display_name: Python (lpccg)
#     language: python
#     name: lpvisitrct
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 3
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython3
#     version: 3.6.5
# ---

# # Secondary Outcomes
# **S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.**
#
# **S2. Total items prescribed per 1000 registered patients for Co-proxamol.**
#
# **S3. Total items prescribed per 1000 registered patients for Dosulepin.**

# +
import pandas as pd
import numpy as np
from lp_measure_conditions import tables

from analysis import compute_regression

import logging
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.ERROR)

GBQ_PROJECT_ID = '620265099307'

# Set dates of baseline and follow-up periods
baseline_start = '2017-01-01'  # baseline start
mid_start = '2017-07-01'  # month after end of baseline period
followup_start = '2018-01-01'  # follow-up start
post_followup_start = '2018-07-01'  # month after end of follow-up period


# -

# ## Prepare data

# +
# Load data which should have been generated already by running the 
# primary outcome notebook
# (Specifically, per-measure cost/items numerators, and population denominators)
data = pd.read_csv("../data/all_measure_data.csv").drop(['Unnamed: 0'], axis=1)

data.head(2)

# +
### select data only for the baseline and follow-up periods

conditions = [
    (data['month'] >= post_followup_start),
    (data['month'] >= followup_start),
    (data['month'] >= mid_start),
    (data['month'] >= baseline_start),
    (data['month'] < baseline_start)]

choices = ['after', 'follow-up', 'mid', 'baseline', 'before']
data['period'] = np.select(conditions, choices, default='0')
data.head(2)
# -

# take columns of interest from df
data = data[[
    "measure",
    "pct_id",
    "period",
    "month",
    "cost",
    "items",
    "denominator"]]
data = data.loc[(data['period']== "baseline") | (data['period']== "follow-up")].set_index(
    ["pct_id", "period", "month"])
data.head(2)

# +
### sum numerator and average population denominators for each CCG for each period
agg_6m = data\
         .groupby(["measure", "pct_id", "period"])\
         .agg({"cost": "sum", "items": "sum", "denominator": "mean"})
### CCGs that have been allocated to the RCT 
rct_ccgs = pd.read_csv('../data/randomisation_group.csv')

# Joint Team information (which CCGs work together in Joint Teams)
team = pd.read_csv('../data/joint_teams.csv')

# Map CCGs to Joint Teams
rct_ccgs = rct_ccgs.merge(team, on="joint_team", how="left")

# Fill blank ccg_ids from joint_id column, so even CCGs not in Joint Teams 
# have a value for joint_id
rct_ccgs["pct_id"] = rct_ccgs["ccg_id"].combine_first(rct_ccgs["joint_id"])
rct_ccgs = rct_ccgs[["joint_id", "allocation", "pct_id"]]

# Combine CCG/Joint Team info with measure data
rct_agg_6m = rct_ccgs.merge(agg_6m.reset_index(), on="pct_id", how="left")
rct_agg_6m.head(3)




agg_6m.head(2)

# +
### CCGs that have been allocated to the RCT 
rct_ccgs = pd.read_csv('../data/randomisation_group.csv')

# Joint Team information (which CCGs work together in Joint Teams)
team = pd.read_csv('../data/joint_teams.csv')

# Map CCGs to Joint Teams
rct_ccgs = rct_ccgs.merge(team, on="joint_team", how="left")

# Fill blank ccg_ids from joint_id column, so even CCGs not in Joint Teams 
# have a value for joint_id
rct_ccgs["pct_id"] = rct_ccgs["ccg_id"].combine_first(rct_ccgs["joint_id"])
rct_ccgs = rct_ccgs[["joint_id", "allocation", "pct_id"]]

# Combine CCG/Joint Team info with measure data
rct_agg_6m = rct_ccgs.merge(agg_6m.reset_index(), on="pct_id", how="left")
rct_agg_6m.head(3)

# -

# aggregate up to Joint team groups
# XXX: SUM both numerators and population denominator across geographies - is this right?
rct_agg_6m = rct_agg_6m\
             .groupby(["joint_id", "allocation", "measure", "period"])\
             .sum()\
             .unstack()\
             .reset_index()
# Rename columns which have awkward names resulting from the unstack operation
rct_agg_6m.columns = rct_agg_6m.columns.map('_'.join).map(lambda x: x.strip("_"))
rct_agg_6m.head(2)


### calculate aggregated measure values (cost only)
rct_agg_6m["baseline_calc_value"] = rct_agg_6m.cost_baseline / rct_agg_6m.denominator_baseline
rct_agg_6m["follow_up_calc_value"] = rct_agg_6m["cost_follow-up"] / rct_agg_6m["denominator_follow-up"]
rct_agg_6m.head(2)

# ## S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.

# find top 3 measures per joint team by cost
top_3 = rct_agg_6m.sort_values(
    by=["joint_id", "baseline_calc_value"], ascending=False)
top_3["measure_rank"] = top_3\
                      .groupby("joint_id")["baseline_calc_value"]\
                      .rank(ascending=False)
top_3 = top_3.loc[top_3.measure_rank <= 3]
top_3.head()

# +
top_3 = top_3\
      .groupby(["joint_id", "allocation"])\
      .agg({"cost_baseline": "sum",
            "cost_follow-up": "sum",
            "denominator_baseline": "mean",
            "denominator_follow-up": "mean"})

### calculate aggregated measure values for combined cost for the top 3 measures
top_3["baseline_calc_value"] = top_3.cost_baseline / top_3.denominator_baseline
top_3["follow_up_calc_value"] = top_3["cost_follow-up"] / top_3["denominator_follow-up"]
top_3.head()
# -

data = top_3.copy().reset_index()
data['intervention'] = data.allocation.map({'con': 0, 'I': 1})
formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(data, formula=formula)


# ## S2: Total items prescribed per 1000 registered patients for Co-proxamol.

# +
# filter data for coproxamol measure:
coprox = rct_agg_6m.loc[rct_agg_6m.measure == "lpcoprox"]

### calculate aggregated measure values (items per 1000 patients)
coprox.loc[:, "baseline_calc_value"] = coprox.loc[:, 'items_baseline'] / coprox.loc[:, "denominator_baseline"]
coprox.loc[:, "follow_up_calc_value"] = coprox.loc[:, "items_follow-up"] / coprox.loc[:, "denominator_follow-up"]
coprox.head()

# +
## Secondary outcome: Total items prescribed per 1000 registered patients for Co-proxamol.
data = coprox.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con': 0, 'I': 1})
formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(data, formula=formula)

# ## S3: Total items prescribed per 1000 registered patients for Dosulepin.
# -

# ## S3: Total items prescribed per 1000 registered patients for Dosulepin

# +
# filter data for dosulepin measure:
dosulepin = rct_agg_6m.copy()
dosulepin = dosulepin.loc[dosulepin.measure == "lpdosulepin"]

### calculate aggregated measure values (items per 1000 patients)
dosulepin["baseline_calc_value"] = dosulepin.items_baseline / dosulepin.denominator_baseline
dosulepin["follow_up_calc_value"] = dosulepin["items_follow-up"] / dosulepin["denominator_follow-up"]
dosulepin.head(2)
# -

## Secondary outcome: Total items prescribed per 1000 registered patients for Dosulepin.
data = dosulepin.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})
formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(data, formula=formula)
