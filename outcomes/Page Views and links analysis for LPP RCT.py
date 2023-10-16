#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


DUMMY_RUN = False  # Change this to False when the analysis is run for real
ANALYTICS_VIEW_ID = '101677264'
GBQ_PROJECT_ID = '620265099307'
get_ipython().run_line_magic('autosave', '0')


# # Engagement outcomes

# In[3]:


# Import page views data from csv
df1 = pd.read_csv(os.path.join('..','data','page_views_ccg.csv'),usecols={"Page","Date","Pageviews","Unique Pageviews"} )
dfp = pd.read_csv(os.path.join('..','data','page_views_practice.csv'),usecols={"Page","Date","Pageviews","Unique Pageviews"} ) 
all_stats = pd.concat([df1,dfp])
all_stats.head()


# In[4]:


# extract if ccg/practice code from path
all_stats["org_id"] = np.where(
    all_stats.Page.str.contains("ccg"),
    all_stats.Page.str.replace('/ccg/', '').str[:3],
    all_stats.Page.str.replace('/practice/', '').str[:6])
all_stats["org_type"] = np.where(
    all_stats.Page.str.contains("ccg"),
    "ccg",
    'practice')

#convert dates to date format
all_stats["Date"] = pd.to_datetime(all_stats["Date"], format='%Y%m%d')

all_stats.head(2)


# In[5]:


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

# Add numerical intervention field
rct_ccgs['intervention'] = rct_ccgs.allocation.map({'con': 0, 'I': 1})

rct_ccgs.head(2)


# In[6]:


## Map practices to joint teams, for practice-level analysis

# Get current mapping data from bigquery
practice_to_ccg = '''select distinct ccg_id, code
from `ebmdatalab.research.practices_2019_09`
where setting = 4 and status_code != 'C'
'''

practice_to_ccg = pd.read_gbq(practice_to_ccg, GBQ_PROJECT_ID, dialect='standard')
practice_to_ccg.to_csv(os.path.join('..','data','practice_to_ccg.csv'))


# In[7]:


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


# In[8]:


# import CCG population sizes

query = '''select prac.ccg_id AS pct_id, sum(stats.total_list_size) as list_size
from `hscic.practice_statistics_all_years` as stats
left join research.practices_2019_09 prac ON stats.practice = prac.code
where CAST(stats.month AS DATE) = '2018-08-01'
group by pct_id
'''
pop = pd.read_gbq(query, GBQ_PROJECT_ID, dialect='standard')
pop.to_csv(os.path.join('..','data','practice_statistics.csv'))


# In[9]:


# merge rct_ccgs with population data
ccg_populations = rct_ccgs.merge(pop, on="pct_id", how="left")[["joint_id", "list_size"]]

# group up to joint teams
joint_team_populations = ccg_populations.groupby("joint_id").sum().reset_index()
joint_team_populations.head()


# In[10]:


# import dates of interventions
visit_dates = pd.read_csv(os.path.join('..','data','allocated_ccgs_visit_timetable.csv'))
visit_dates["date"] = pd.to_datetime(visit_dates.date)

# merge with rct_ccgs/joint teams
allocations_with_dates = rct_ccgs.merge(visit_dates, on="joint_id", how="left").drop("pct_id", axis=1).drop_duplicates()
allocations_with_dates_and_sizes = allocations_with_dates.merge(joint_team_populations, on="joint_id")

# rank by size, to allow us to pair similar interventions and controls
allocations_with_dates_and_sizes["size_rank"] = allocations_with_dates_and_sizes.groupby("allocation").list_size.rank()

# assign dummy intervention dates to control practices by pairing on total list size
i_group = allocations_with_dates_and_sizes[["allocation", "date", "size_rank"]]          .loc[allocations_with_dates_and_sizes.allocation == "I"]          .drop("allocation", axis=1)

allocations_with_dates_and_sizes = allocations_with_dates_and_sizes.merge(i_group, on="size_rank", how="left", suffixes=["", "_int"])         .drop("date", axis=1)         .sort_values(by=["size_rank", "allocation"])
allocations_with_dates_and_sizes.head()


# In[11]:


# join joint-group / ccg allocations, visit dates and list size info to page views data
all_data = allocations_with_dates_and_sizes.drop("size_rank", axis=1)       .merge(
           stats_with_allocations.drop(["allocation", "pct_id", "ccg_id", "intervention"], axis=1),
           how='left',
           on='joint_id')
all_data.head(2)


# In[12]:


# assign each page view occurrence to before vs after intervention (1 month ~ 28 days)

all_data["datediff"] = all_data.Date - all_data.date_int
all_data["timing"] = "none"
all_data.loc[(all_data.datediff <= "28 days") & (all_data.datediff > "0 days"),
      "timing"] = "after"
all_data.loc[(all_data.datediff >= "-28 days") & (all_data.datediff < "0 days"),
      "timing"] = "before"
all_data["Unique Pageviews"] = all_data["Unique Pageviews"].fillna(0)
all_data.head(2)


# In[13]:


# group up page views data to joint teams and sum page views before
# and after interventions

all_data_agg = all_data.groupby(["intervention", "joint_id", "org_type", "list_size", "timing"])      .agg({"Unique Pageviews": sum, "Page": "nunique"}).unstack().unstack(2).fillna(0).stack()
# added an extra unstack and stack here to fill nulls with zero and ensure each joint_id always counted in its allocation group
#even if it does not have any page views in either the ccg or practice org_type

all_data_agg = all_data_agg.rename(columns={"Page": "No_of_Pages"}).reset_index()
#flatten columns and drop superfluous columns
all_data_agg.columns = all_data_agg.columns.map('_'.join).map(lambda x: x.strip("_"))
all_data_agg = all_data_agg.drop(["Unique Pageviews_none","No_of_Pages_none"], axis=1)
all_data_agg.head()


# ## Engagement outcome E1
# Number of page views over one month on CCG pages showing low-priority measures, before vs after intervention, between intervention and control groups.
# 
# 

# In[14]:


# filter CCG page views only:
ccg_data_agg = all_data_agg.loc[all_data_agg.org_type == "ccg"]
ccg_data_agg_trimmed = trim_5_percentiles(ccg_data_agg, debug=False)

display(ccg_data_agg_trimmed.groupby("intervention").agg({"joint_id":"nunique",
                                                            "Unique Pageviews_before":{"sum","mean","std"},
                                                            "Unique Pageviews_after":{"sum","mean","std"}}))

formula = ('data["proxy_pageviews_after"] '
           ' ~ data["proxy_pageviews_before"] + intervention')
compute_regression(
    ccg_data_agg_trimmed,
    formula=formula)


# ## Engagement outcome E2
# Number of page views over one month on practice pages showing low-priority measures, grouped up to CCGs

# In[15]:


practice_data_agg = all_data_agg.loc[all_data_agg.org_type == "practice"]
practice_data_agg_trimmed = trim_5_percentiles(practice_data_agg, debug=False)

display(practice_data_agg_trimmed.groupby("intervention").agg({"joint_id":"nunique",
                                                        "Unique Pageviews_before":{"sum","mean","std"},
                                                        "Unique Pageviews_after":{"sum","mean","std"}}))

compute_regression(
    practice_data_agg_trimmed,
    formula=formula)


# # Engagement outcomes E3 and E4 : Alert sign-ups
# 

# ## Prepare data

# In[16]:


# import data from django administration, filtered for confirmed sign-ups only (no date filter)

alerts = pd.read_csv(os.path.join('..','data','orgbookmarks-2019-04-30.csv'))
alerts["created_at"] = pd.to_datetime(alerts.created_at).dt.strftime('%Y-%m-%d')

alerts = alerts.loc[alerts["approved"]=="t"] 
## added this extra step to filter for confirmed sign ups as download from archive is not pre-filtered

alerts = alerts.rename(columns={"pct_id":"pct", "practice_id":"practice", "user_id":"user"})
alerts.head()


# In[17]:


# map practices to joint teams (and thus only include RCT subjects)
alerts = alerts.merge(
    rct_practices[["ccg_id", "code"]],
    left_on="practice",
    right_on="code",
    how="left").drop("code",axis=1)
# Fill nulls in ccg_id column from values in pct colume
alerts.ccg_id = alerts.ccg_id.combine_first(alerts.pct)
alerts.head()


# In[18]:


# Add RCT allocations to data
alerts = rct_ccgs.merge(alerts, left_on="pct_id", right_on="ccg_id", how="left")
# flag whether each alert is a practice or CCG alert
conditions = [
    (alerts.pct.str.len()==3),
    (alerts.practice.str.len()==6)]

choices = ['ccg', 'practice']
alerts['org_type'] = np.select(conditions, choices, default='none')
alerts.head()


# In[19]:


# join to visit dates
alerts_with_dates_and_stats = allocations_with_dates_and_sizes                              .drop(["size_rank", "allocation", "intervention"],axis=1)                              .merge(alerts.drop(["approved"], axis=1),
                                     how='left', on='joint_id')
alerts_with_dates_and_stats.head()


# In[20]:


# assign each page view occurrence to before vs after intervention (1
# month ~ 28 days)
alerts_with_dates_and_stats["created_at"] = pd.to_datetime(alerts_with_dates_and_stats.created_at)
alerts_with_dates_and_stats["datediff"] = (
    alerts_with_dates_and_stats.created_at - alerts_with_dates_and_stats.date_int)
alerts_with_dates_and_stats["timing"] = "none"
# all alerts set up prior to day of intervention will be used as a co-variable:
alerts_with_dates_and_stats.loc[
    (alerts_with_dates_and_stats.datediff < "0 days"),
    "timing"] = "before"


# In[21]:


# main outcome: alerts set up within 3 months of intervention:
alerts_with_dates_and_stats.loc[
    (alerts_with_dates_and_stats.datediff >= "0 days") &
    (alerts_with_dates_and_stats.datediff <= "84 days"),
    "timing"] = "after"  # (within 3 months)


# In[22]:


# aggregate data: sum alerts before and after intervention for each joint team
alerts_agg = alerts_with_dates_and_stats     .groupby(["intervention", "joint_id", "list_size", "timing", "org_type"])     .agg({"user": "nunique"})     .unstack()     .fillna(0)
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


# In[23]:


# summary data
display(alerts_agg.groupby("intervention")[["joint_id"]].nunique())
alerts_agg.groupby("intervention").agg(["mean","std"])


# ### E3 Number of registrations to OpenPrescribing CCG email alerts

# In[24]:


formula = ('data["alerts_ccg_after"] ~ '
           'data["alerts_ccg_before"] + list_size_100k + intervention')
compute_regression(
    alerts_agg,
    formula=formula)


# 
# ### E4 Number of registrations to OpenPrescribing Practice email alerts grouped up to CCG
# (New sign-ups within 3 months of intervention. The CCG registered population and number of sign-ups prior to the intervention will be co-variables.)

# In[25]:


formula = ('data["alerts_practice_after"] ~ '
           'data["alerts_practice_before"] + list_size_100k + intervention')
compute_regression(
    alerts_agg,
    formula=formula)

