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
import os
import requests
import pandas as pd
import numpy as np

from analysis import compute_regression

import logging
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.ERROR)

GBQ_PROJECT_ID = '620265099307'
DUMMY_RUN = True  # Useful for testing; set to false when doing real analysis

# Set dates of baseline and follow-up periods
baseline_start = '2017-01-01'       # baseline start
mid_start = '2017-07-01'            # month after end of baseline period
followup_start = '2018-01-01'       # follow-up start
post_followup_start = '2018-07-01'  # month after end of follow-up period

all_measures = ['lpcoprox', 'lpdosulepin', 'lpdoxazosin', 
                'lpfentanylir', 'lpglucosamine', 'lphomeopathy', 
                'lplidocaine', 'lpliothyronine', 'lplutein', 
                'lpomega3', 'lpoxycodone', 'lpperindopril', 
                'lprubefacients', 'lptadalafil', 'lptramadolpara', 
                'lptravelvacs', 'lptrimipramine']
definition_url = (
    "https://raw.githubusercontent.com/ebmdatalab/openprescribing/"
    "{commit}/openprescribing/frontend/management/commands/measure_definitions/"
    "{measure}.json")
commit_for_measure_definitions = "6f949660fee06401102136926eaba075d963511d"


# -

# Import data from BigQuery
# (Specifically, per-measure cost/items numerators, and population denominators)
if DUMMY_RUN and os.path.exists("../data/all_measure_data.csv"):
    rawdata = pd.read_csv("../data/all_measure_data.csv").drop(['Unnamed: 0'], axis=1)
else:
    rawdata = pd.DataFrame()
    sql_template = open("measure.sql", "r").read()
    for measure in all_measures:
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
    rawdata.to_csv("../data/all_measure_data.csv")
rawdata.head(1)

# Aggregate across all measures 
data = rawdata.groupby(["pct_id", "month"]).agg(
    {'items':'sum', 'cost': 'sum', 'denominator':'first'}).reset_index()
data = data.rename(columns={"cost": "numerator"})
data['calc_value'] = data['numerator'] / data['denominator']
data.head(2)

# +
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
# -

# group measurements for each CCG for each period
agg_6m = data.groupby(["pct_id", "period"]).agg(
    {"numerator": "sum", "items": "sum", "denominator": "mean"})
agg_6m.head()

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
# XXX: SUM both numerator and population denominator across geographies - is this right?
rct_agg_6m = rct_agg_6m\
             .groupby(["joint_id", "allocation", "period"])\
             .sum()\
             .unstack()\
             .reset_index()
# Rename columns which have awkward names resulting from the unstack operation
rct_agg_6m.columns = rct_agg_6m.columns.map('_'.join).map(lambda x: x.strip("_"))
# Create binary "intervention" column for later regression
rct_agg_6m['intervention'] = rct_agg_6m.allocation.map({'con': 0, 'I': 1})
rct_agg_6m.head(3)


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

rct_agg_6m.head(3)
# -

# # Primary Outcome P1
#
# Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined, between intervention and control groups, assessed by applying a multivariable linear regression model.
#

formula = ('data["follow_up_calc_value"] '
           '~ data["baseline_calc_value"] + intervention')
compute_regression(rct_agg_6m, formula=formula)

# # Primary Outcome P2 
# ITEMS per 1,000 patients for all 18 pre-specified “low-priority” treatments combined, between intervention and control groups, assessed by applying a multivariable linear regression model.
#

formula = ('data["follow_up_items_thou"] '
           '~ data["baseline_items_thou"] + intervention')
compute_regression(rct_agg_6m, formula=formula)
