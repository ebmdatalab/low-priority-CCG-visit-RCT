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

# # Primary outcomes
# **P1.  Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined.**
#
# **P2. Total items per 1000 across all 18 low priority treatments.**
#

# %autosave 0

# +
import datetime
import pandas as pd
import numpy as np
from lp_measure_conditions import tables

from analysis import compute_regression
from analysis import trim_5_percentiles

import logging
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.ERROR)

GBQ_PROJECT_ID = '620265099307'


# Set dates of baseline and follow-up periods
baseline_start = '2017-01-01'  # baseline start
mid_start = '2017-07-01'  # month after end of baseline period
followup_start = '2018-01-01'  # follow-up start
post_followup_start = '2018-07-01'  # month after end of follow-up period

# Import dataset from BigQuery

# costs (totals, not divided into individual measures) - for P1
costs_sql = '''SELECT * FROM ebmdatalab.measures.ccg_data_lpzomnibus
WHERE EXTRACT (YEAR from month)  >= 2017
'''

# items (summed across all lp measures) - for P2:
# this is the total number of items in each lp measure
where = []
for table, condition in tables.items():
    where.append(condition)
items_sql = '''SELECT pct AS pct_id, month, sum(items) AS items FROM
  `ebmdatalab.hscic.normalised_prescribing_standard`
  WHERE  month >= '{}' AND ({})
  GROUP BY pct, month'''.format(baseline_start, " OR ".join(where))
data = pd.read_gbq(costs_sql, GBQ_PROJECT_ID, dialect='standard')
data["month"] = pd.to_datetime(data.month)
data.to_csv("../data/lowpriory_costs.csv")
items = pd.read_gbq(items_sql, GBQ_PROJECT_ID, dialect='standard')
items["month"] = pd.to_datetime(items.month)
items.to_csv("../data/lowpriory_items.csv")

# -

# merge items and costs data
data = data.merge(
    items,
    on=["pct_id", "month"],
    how="outer").sort_values(by=["pct_id", "month"])
data.head()

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
# -

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
# Restrict to columns of interest
data = data[["pct_id", "period", "month", "numerator", "denominator", "items"]]
data = data.loc[
    (data['period'] == "baseline") | (data['period'] == "follow-up")
].set_index(["pct_id", "period", "month"])

data.head()
# -

### group measurements for each CCG for each period
agg_6m = data.groupby(["pct_id", "period"]).agg(
    {"numerator": sum, "items": sum, "denominator": "mean"})
agg_6m.head()

# +
### import **allocated** Rct_Ccgs
rct_ccgs = pd.read_csv('../data/randomisation_group.csv')

# joint team information
team = pd.read_csv('../data/joint_teams.csv')

# create map of rct_ccgs to joint teams
rct_ccgs = rct_ccgs.merge(team, on="joint_team", how="left")

# fill blank ccg_ids from joint_id column, so every CCG has a value
# for joint_id
rct_ccgs["pct_id"] = rct_ccgs["ccg_id"].combine_first(rct_ccgs["joint_id"])
rct_ccgs = rct_ccgs[["joint_id", "allocation", "pct_id"]]

# merge rct_ccgs with data
rct_agg_6m = rct_ccgs.merge(agg_6m.reset_index(), on="pct_id", how="left")
rct_agg_6m.head()

# -

# group up to Joint team groups
# note: SUM both numerator and population denominator across geographies
rct_agg_6m = rct_agg_6m\
             .groupby(["joint_id", "allocation", "period"])\
             .sum()\
             .unstack()\
             .reset_index()
rct_agg_6m.columns = rct_agg_6m.columns.map('_'.join).map(lambda x: x.strip("_"))
rct_agg_6m['intervention'] = rct_agg_6m.allocation.map({'con': 0, 'I': 1})
rct_agg_6m.head()


# +
# calculate aggregated measure values for baseline and followup pareiods
rct_agg_6m["baseline_calc_value"] = (
    rct_agg_6m.numerator_baseline / rct_agg_6m.denominator_baseline)
rct_agg_6m["follow_up_calc_value"] = (
    rct_agg_6m["numerator_follow-up"] / rct_agg_6m["denominator_follow-up"])
rct_agg_6m["baseline_items_thou"] = (
    rct_agg_6m.items_baseline / rct_agg_6m.denominator_baseline)
rct_agg_6m["follow_up_items_thou"] = (
    rct_agg_6m["items_follow-up"] / rct_agg_6m["denominator_follow-up"])

rct_agg_6m.head()

# +
### Primary Outcome ########################
# Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined,
# between intervention and control groups, assessed by applying a multivariable linear regression model.

formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(rct_agg_6m, formula=formula)

# +
### Primary Outcome P2 ########################
# ITEMS per 1,000 patients for all 18 pre-specified “low-priority” treatments combined,
# between intervention and control groups, assessed by applying a multivariable linear regression model.

formula = ('data["follow_up_items_thou"] '
           '~ data["baseline_items_thou"] + intervention')
compute_regression(rct_agg_6m, formula=formula)
