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

# +
# Imports and variables
import os

from analysis import compute_regression
from analysis import trim_5_percentiles

import pandas as pd
import numpy as np
import analytics

import logging
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.ERROR)
# -

DUMMY_RUN = True  # Change this to False when the analysis is run for real
ANALYTICS_VIEW_ID = '101677264'
GBQ_PROJECT_ID = '620265099307'
# %autosave 0

# # Engagement outcomes

# Import page views data
#
# Timepoints:
# - 1 month before/after
# - April-Sept 2018 vs April-Sept 2019
#
from importlib import reload
reload(analytics)
if DUMMY_RUN and os.path.exists("../data/pageview_stats.csv"):
    # CCG-level data:
    all_stats = pd.read_csv("../data/pageview_stats.csv",usecols={"Page","Date","Pageviews","Unique Pageviews"})
    all_stats['Date'] = pd.to_datetime(all_stats['Date'])
else:
    ccg_query = [
        {
            'viewId': ANALYTICS_VIEW_ID,
            "samplingLevel": "LARGE",
            'dateRanges': [
                {'startDate': '2018-04-01',
                 'endDate': '2019-09-30'}
            ],
            'metrics': [
                {'expression': 'ga:pageViews'},
                {'expression': 'ga:uniquePageViews'},
            ],
            "dimensions": [
                {"name": "ga:pagePath"},
                {"name": "ga:date"},
            ],
            "dimensionFilterClauses": [{
                "operator": "AND",
                "filters": [
                    {
                        "dimensionName": "ga:pagePath",
                        "operator": "REGEXP",
                        "expressions": ["^/(ccg|practice).*lowp"]
                    },
                    {
                        "dimensionName": "ga:pagePath",
                        "not": True,
                        "operator": "PARTIAL",
                        "expressions": ["analyse"]
                    }
                ]
            }]
        }]
    colnames = ["Date", "Page", "Pageviews", "Unique Pageviews"]
    all_stats = analytics.query_analytics(ccg_query, columns=colnames)
    all_stats.to_csv("../data/pageview_stats.csv")

# extract if ccg/practice code from path
all_stats["org_id"] = np.where(
    all_stats.Page.str.contains("ccg"),
    all_stats.Page.str.replace('/ccg/', '').str[:3],
    all_stats.Page.str.replace('/practice/', '').str[:6])
all_stats["org_type"] = np.where(
    all_stats.Page.str.contains("ccg"),
    "ccg",
    'practice')
all_stats.head(2)

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

# Add numerical intervention field
rct_ccgs['intervention'] = rct_ccgs.allocation.map({'con': 0, 'I': 1})

rct_ccgs.head(2)

# +
## Map practices to joint teams, for practice-level analysis

# Get current mapping data from bigquery
practice_to_ccg = '''select distinct ccg_id, code
from `ebmdatalab.hscic.practices`
where setting = 4 and status_code != 'C'
'''

practice_to_ccg = pd.read_gbq(practice_to_ccg, GBQ_PROJECT_ID, dialect='standard')
practice_to_ccg.to_csv("../data/practice_to_ccg.csv")
# -

# extract practice statistics for practices that are members of CCGs who are in the RCT
rct_practices = rct_ccgs[["pct_id"]].merge(practice_to_ccg, left_on="pct_id", right_on ="ccg_id", how="left")
# add a new "ccg_id" column just for practices
all_stats_with_ccg = all_stats.merge(
    rct_practices[["ccg_id", "code"]],
    left_on="org_id",
    right_on="code",
    how="left").drop("code", axis=1)
all_stats_with_ccg.loc[all_stats_with_ccg.org_id.str.len() == 3, "ccg_id"] = all_stats_with_ccg.org_id
# Add joint team id and allocation onto the new stats
stats_with_allocations = rct_ccgs.merge(all_stats_with_ccg, left_on="pct_id",right_on="ccg_id",how="left")

# +
# import CCG population sizes

query = '''select pct_id, sum(total_list_size) as list_size
from `hscic.practice_statistics` as stats
where CAST(month AS DATE) = '2018-08-01'
group by pct_id
'''
pop = pd.read_gbq(query, GBQ_PROJECT_ID, dialect='standard')
pop.to_csv("../data/practice_statistics.csv")

# +
# merge rct_ccgs with population data
ccg_populations = rct_ccgs.merge(pop, on="pct_id", how="left")[["joint_id", "list_size"]]

# group up to joint teams
joint_team_populations = ccg_populations.groupby("joint_id").sum().reset_index()
joint_team_populations.head()


# +
# import dates of interventions
visit_dates = pd.read_csv('../data/allocated_ccgs_visit_timetable.csv')
visit_dates["date"] = pd.to_datetime(visit_dates.date)

# merge with rct_ccgs/joint teams
allocations_with_dates = rct_ccgs.merge(visit_dates, on="joint_id", how="left").drop("pct_id", axis=1).drop_duplicates()
allocations_with_dates_and_sizes = allocations_with_dates.merge(joint_team_populations, on="joint_id")

# rank by size, to allow us to pair similar interventions and controls
allocations_with_dates_and_sizes["size_rank"] = allocations_with_dates_and_sizes.groupby("allocation").list_size.rank()

# assign dummy intervention dates to control practices by pairing on total list size
i_group = allocations_with_dates_and_sizes[["allocation", "date", "size_rank"]]\
          .loc[allocations_with_dates_and_sizes.allocation == "I"]\
          .drop("allocation", axis=1)

allocations_with_dates_and_sizes = allocations_with_dates_and_sizes.merge(i_group, on="size_rank", how="left", suffixes=["", "_int"])\
         .drop("date", axis=1)\
         .sort_values(by=["size_rank", "allocation"])
allocations_with_dates_and_sizes.head()
# -

# join joint-group / ccg allocations, visit dates and list size info to page views data
all_data = allocations_with_dates_and_sizes.drop("size_rank", axis=1)\
       .merge(
           stats_with_allocations.drop(["allocation", "pct_id", "ccg_id", "intervention"], axis=1),
           how='left',
           on='joint_id')
all_data.head(2)

# +
# assign each page view occurrence to before vs after intervention (1 month ~ 28 days)

all_data["datediff"] = all_data.Date - all_data.date_int
all_data["timing"] = "none"
all_data.loc[(all_data.datediff <= "28 days") & (all_data.datediff > "0 days"),
      "timing"] = "after"
all_data.loc[(all_data.datediff >= "-28 days") & (all_data.datediff < "0 days"),
      "timing"] = "before"
all_data["Unique Pageviews"] = all_data["Unique Pageviews"].fillna(0)
all_data.head(2)

# +
# group up page views data to joint teams and sum page views before
# and after interventions

all_data_agg = all_data.groupby(["intervention", "joint_id", "org_type", "list_size", "timing"])\
      .agg({"Unique Pageviews": sum, "Page": "nunique"}).unstack().fillna(0)
all_data_agg = all_data_agg.rename(columns={"Page": "No_of_Pages"}).reset_index()
#flatten columns and drop superfluous columns
all_data_agg.columns = all_data_agg.columns.map('_'.join).map(lambda x: x.strip("_"))
all_data_agg = all_data_agg.drop(["Unique Pageviews_none","No_of_Pages_none"], axis=1)
all_data_agg.head()
# -

# ## Engagement outcome E1
# Number of page views over one month on CCG pages showing low-priority measures, before vs after intervention, between intervention and control groups.
#
#

# filter CCG page views only:
ccg_data_agg = all_data_agg.loc[all_data_agg.org_type == "ccg"]
ccg_data_agg_trimmed = trim_5_percentiles(ccg_data_agg, debug=False)
formula = ('data["proxy_pageviews_after"] '
           ' ~ data["proxy_pageviews_before"] + intervention')
compute_regression(
    ccg_data_agg_trimmed,
    formula=formula)

# ## Engagement outcome E2
# Number of page views over one month on practice pages showing low-priority measures, grouped up to CCGs

practice_data_agg = all_data_agg.loc[all_data_agg.org_type == "practice"]
practice_data_agg_trimmed = trim_5_percentiles(practice_data_agg, debug=False)
compute_regression(
    practice_data_agg_trimmed,
    formula=formula)


# # Engagement outcomes E3 and E4 : Alert sign-ups
#

# ## Prepare data

# +
# import data from django administration, filtered for confirmed sign-ups only (no date filter)

alerts = pd.read_csv('../data/OrgBookmark-2018-11-02.csv')
alerts["created_at"] = pd.to_datetime(alerts.created_at)

alerts.head()

# -


# map practices to joint teams (and thus only include RCT subjects)
alerts = alerts.merge(
    rct_practices[["ccg_id", "code"]],
    left_on="practice",
    right_on="code",
    how="left").drop("code",axis=1)
# Fill nulls in ccg_id column from values in pct colume
alerts.ccg_id = alerts.ccg_id.combine_first(alerts.pct)
alerts.head()

# +
# Add RCT allocations to data
alerts = rct_ccgs.merge(alerts, left_on="pct_id", right_on="ccg_id", how="left")
# flag whether each alert is a practice or CCG alert
conditions = [
    (alerts.pct.str.len()==3),
    (alerts.practice.str.len()==6)]

choices = ['ccg', 'practice']
alerts['org_type'] = np.select(conditions, choices, default='none')
alerts.head()
# -

# join to visit dates
alerts_with_dates_and_stats = allocations_with_dates_and_sizes\
                              .drop(["size_rank", "allocation", "intervention"],axis=1)\
                              .merge(alerts.drop(["approved"], axis=1),
                                     how='left', on='joint_id')
alerts_with_dates_and_stats.head()

# assign each page view occurrence to before vs after intervention (1
# month ~ 28 days)
alerts_with_dates_and_stats["datediff"] = (
    alerts_with_dates_and_stats.created_at - alerts_with_dates_and_stats.date_int)
alerts_with_dates_and_stats["timing"] = "none"
# all alerts set up prior to day of intervention will be used as a co-variable:
alerts_with_dates_and_stats.loc[
    (alerts_with_dates_and_stats.datediff < "0 days"),
    "timing"] = "before"

# main outcome: alerts set up within 3 months of intervention:
alerts_with_dates_and_stats.loc[
    (alerts_with_dates_and_stats.datediff >= "0 days") &
    (alerts_with_dates_and_stats.datediff <= "84 days"),
    "timing"] = "after"  # (within 3 months)


# +
# aggregate data: sum alerts before and after intervention for each joint team
alerts_agg = alerts_with_dates_and_stats\
     .groupby(["intervention", "joint_id", "list_size", "timing", "org_type"])\
     .agg({"user": "nunique"})\
     .unstack()\
     .fillna(0)
alerts_agg = alerts_agg.rename(columns={"user": "alerts"}).unstack().reset_index().fillna(0)

# flatten columns:
alerts_agg.columns = alerts_agg.columns.map('_'.join).map(lambda x: x.rstrip("_"))

alerts_agg["list_size_100k"] = alerts_agg["list_size"]/100000
alerts_agg = alerts_agg[
    ["intervention",
     "joint_id",
     "list_size_100k",
     "alerts_ccg_after",
     "alerts_ccg_before",
     "alerts_practice_after",
     "alerts_practice_before"]]



alerts_agg.head()
# -

# summary data
alerts_agg.groupby("intervention").mean()

# ### E3 Number of registrations to OpenPrescribing CCG email alerts

formula = ('data["alerts_ccg_after"] ~ '
           'data["alerts_ccg_before"] + data["list_size_100k"] + intervention')
compute_regression(
    alerts_agg,
    formula=formula)


#
# ### E4 Number of registrations to OpenPrescribing Practice email alerts grouped up to CCG
# (New sign-ups within 3 months of intervention. The CCG registered population and number of sign-ups prior to the intervention will be co-variables.)

formula = ('data["alerts_practice_after"] ~ '
           'data["alerts_practice_before"] + data["list_size_100k"] + intervention')
compute_regression(
    alerts_agg,
    formula=formula)
