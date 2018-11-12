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

# # Allocation of CCGs into intervention and control groups

# Note - set seed for random allocation to ensure repeatability

# +
# Set dates of baseline and follow-up periods
d4 = '2019-07-01' # month after end of follow-up period
d3 = '2019-01-01' # follow-up start
d2 = '2018-07-01' # month after end of baseline period
d1 = '2018-01-01' # baseline start


# Import dataset from BigQuery
import pandas as pd
import numpy as np
GBQ_PROJECT_ID = '620265099307'

q = '''SELECT * FROM ebmdatalab.measures.ccg_data_lpzomnibus
WHERE month >= '2018-01-01' AND month <= '2018-08-01'
'''
df1 = pd.read_gbq(q, GBQ_PROJECT_ID, dialect='standard',verbose=False)

df1["month"] = pd.to_datetime(df1.month)

df1.head() # this gives the first few rows of data

# +
### classify the data by period
import datetime

conditions = [
    (df1['month']  >= d4), # after follow-up period
    (df1['month']  >= d3), # follow-up
    (df1['month']  >= d2), # mid
    (df1['month']  >= d1), # baseline
    (df1['month']  < d1)] # before

choices = ['after', 'follow-up', 'mid', 'baseline','before']
df1['period'] = np.select(conditions, choices, default='0')

df1.head()

# +
### aggregate the data over the each period, and 
### then extract just the 6 months of baseline data

# take columns of interest from df
df2 = df1[["pct_id","period", "month", "numerator","denominator"]]

# Perform groupby aggregation
agg_6m = df2.groupby(["pct_id","period"]).sum() 

### calculate aggregated measure values
agg_6m["calc_value"] = agg_6m.numerator / agg_6m.denominator

agg_6m = agg_6m.reset_index()
agg_6m = agg_6m.loc[agg_6m.period=="baseline"].rename(columns={"calc_value":"baseline"}).drop("period",axis=1)
agg_6m.head()

# +
### select the worst ~50 to be pre-screened
#(Also exclude CCGs 99P, 99Q and 08H as per exclusion criteria)

df3 = agg_6m.copy()
df3.loc[(df3.pct_id!="08H")&(df3.pct_id !="99P")&(df3.pct_id !="99Q")].sort_values(by="baseline", ascending=False).head(50).reset_index()

# -

# ### The selected CCGs are pre-screened for joint medicines optimisation teams
#
# Specifically, the 50 CCGs above were reviewed by a pharmacist for membership of joint medicines optimisations teams.  The pharmacist created a spreadsheet indicating membership, `joint_teams.csv`, used in the following cells.
#
# This is to avoid contamination between CCGs that work together. Therefore, we block randomise taking these teams into account. 
#

# +
# import joint team information
team = pd.read_csv('joint_teams.csv')

# give each team a proxy id, i.e. where there are teams, assign the 
# code of its members to the entire team. This  member becomes the 
# CCG we visit as the intervention for that team.
team2 = pd.DataFrame(team.groupby("joint_team")["ccg_id"].agg(["count","max"])).reset_index().rename(columns={"max":"joint_id"})
team = team.merge(team2, on="joint_team")
team.head()
# -

# merge aggregated prescribing data with joint team information
j1 = agg_6m.merge(team, left_on="pct_id",right_on="ccg_id", how="left")
j1.loc[j1.ccg_id.isnull(),["joint_id"]] = j1.pct_id
j1 = j1.drop("ccg_id", axis=1)
j1.head()

# group CCG data up to joint teams
j2 = j1.groupby("joint_id")["numerator","denominator"].sum().reset_index()
j2["baseline"] = j2.numerator / j2.denominator
j2.head()

# +
### calculate percentile for each ccg / joint team for spend during baseline period 
# and select the worst 40 to be randomised

j3 = j2.copy()
j3["baseline_ranking"] = j3["baseline"].rank(method='min', pct=True)

top40 = j3.loc[(j3.joint_id!="08H")&(j3.joint_id !="99P")&(j3.joint_id !="99Q")].sort_values(by="baseline_ranking", ascending=False).head(40).reset_index(drop=True)
top40
# -

top40.describe()

# +
### allocate bottom CCGs to intervention and control groups 

# set seeds for random number generation to ensure repeatable
seed1 = 321

df5 = top40.copy()
import random as rd

np.random.seed(seed1)
df5['rand_num'] = np.random.rand(len(df5))
df5["allocation_ranking"] = df5.rand_num.rank()

df5["allocation_code"]= df5.allocation_ranking.mod(2)

#create final allocation groups
df5['allocation'] = np.where(df5['allocation_code']==0,'con','I')

print (df5.loc[df5.allocation=="I"].joint_id.count(), 'CCGs have been assigned to the intervention group,')
print ("with an average spend of £",round(df5.loc[df5.allocation=="I"].baseline.mean(),0), "per 1000. SD:",round(df5.loc[df5.allocation=="I"].baseline.std(),0))
print (df5.loc[df5.allocation=="con"].joint_id.count(), 'CCGs have been assigned to the control group,')
print ("with an average spend of £",round(df5.loc[df5.allocation=="con"].baseline.mean(),0), "per 1000. SD:",round(df5.loc[df5.allocation=="con"].baseline.std(),0))


# +
### import CCG names for CCGs allocated to intervention group
q = '''
SELECT
  code,
  name
FROM
  ebmdatalab.hscic.ccgs
WHERE org_type = "CCG" 
'''

ccg = pd.io.gbq.read_gbq(q, GBQ_PROJECT_ID, dialect='standard',verbose=False)

ccg.head()
dfm = df5.loc[df5.allocation == "I"].merge(ccg, how='left', left_on='joint_id',right_on='code')

#also join back to joint team info and show whether the joint team has any other CCGs in the eligilbe group.
dfm = dfm[["joint_id","name"]].merge(team2, on="joint_id", how="left").sort_values(by="joint_team").rename(columns={"count":"CCGs_included"})
dfm

dfm.to_csv('allocated_ccgs_visit.csv')
# -

# ### Calculate baseline stats for whole population, to use to give context in power calculation

j3["baseline"].describe(percentiles = [.1, .25, .5, .75, .8,.85, .9])


