# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
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

# # Engagement outcomes 
#
# For the primary and some secondary outcomes, we need to use Google Analytics page views data:
#
# ## E1: Number of page views over one month on CCG page showing low-priority measures
#
# Timepoints: 
# - 1 month before/after
# - April-Sept 2018 vs April-Sept 2019
#
# **Analytics data extraction procedure:**
#
# `Analytics - Behaviour - Site Content - All Pages`
#  
#  ```
# ADVANCED SEARCH: Page matching regexp "/ccg"  AND  Matching regexp "lowp" AND Exclude page including "analyse"
#  SECONDARY DIMENSION: "DATE"
#  DATE RANGE: Sept 2018 - Jan 2019 (visits take place Oct-Dec 2018); also April-Sept 2018 and April-Sept 2019
#  SHOW ROWS: 5000
#  ```
#  Export as CSV
#  Before importing, tidy up the csv to create a flat table
#  (remove top and bottom groups of rows, convert numerical data to general format to remove commas)
#  
# ## E2: Number of page views over one month on practice pages showing low-priority measures, grouped up to CCGs
#
# Timepoints: 
# - 1 month before/after
# - April-Sept 2018 vs April-Sept 2019
#
#  
# **Analytics data extraction procedure:**
# `Analytics - Behaviour - Site Content - All Pages`
#  
#  ```
# ADVANCED SEARCH: Page matching regexp "/practice"  AND  Matching regexp "lowp" AND Exclude page including "analyse"
#  SECONDARY DIMENSION: "DATE"
#  DATE RANGE: Sept 2018 - Jan 2019 (visits take place Oct-Dec 2018); also April-Sept 2018 and April-Sept 2019
#  SHOW ROWS: 5000
#  ```
#  
#  Export as CSV
#
#  Before importing, tidy up the csv to create a flat table
#  (remove top and bottom groups of rows, convert numerical data to general format to remove commas)
#

# +
# Import page views data
import pandas as pd
import numpy as np

# CCG-level data:
df1 = pd.read_csv('page_views_dummy_ccg.csv',usecols={"Page","Date","Pageviews","Unique Pageviews"} )
# practice-level data:
dfp = pd.read_csv('page_views_dummy_practice.csv',usecols={"Page","Date","Pageviews","Unique Pageviews"} )

df1 = pd.concat([df1,dfp])
df1.head()
# -

# convert Date field to date format
df1["date"] = df1.Date.apply(str).str[:4] + '-' + df1.Date.apply(str).str[4:6] + '-' + df1.Date.apply(str).str[6:]
df1["date"] = pd.to_datetime(df1.date)
df1 = df1.drop("Date",axis=1)
df1 = df1.loc[df1.Page.str.contains("lowp")]
df1.head()

# extract ccg/practice code from path
df1["org_id"] = np.where(df1.Page.str.contains("ccg"),df1.Page.str.replace('/ccg/', '').str[:3],df1.Page.str.replace('/practice/', '').str[:6])
df1["org_type"] = np.where(df1.Page.str.contains("ccg"),"ccg",'practice')
df1.head()

# +
GBQ_PROJECT_ID = '620265099307'

# import practice-CCG mapping
mapp = '''select distinct ccg_id, code
from `ebmdatalab.hscic.practices` 
where setting = 4 and status_code != 'C'
'''
mapp = pd.read_gbq(mapp, GBQ_PROJECT_ID, dialect='standard',verbose=False)

### import **allocated** CCGs
ccgs = pd.read_csv('randomisation_group.csv')
# import joint team information
team = pd.read_csv('joint_teams.csv')

# create map of ccgs to joint teams
ccgs = ccgs.merge(team,on="joint_team", how="left")
#fill blank ccg_ids from joint_id column
ccgs["pct_id"] = ccgs["ccg_id"].combine_first(ccgs["joint_id"])
ccgs = ccgs[["joint_id","allocation","pct_id"]]
ccgs.head()
# -

# map practices onto CCGs
map2 = ccgs[["pct_id"]].merge(mapp, left_on="pct_id", right_on ="ccg_id", how="left")
df2 = df1.merge(map2[["ccg_id","code"]], left_on="org_id",right_on="code", how="left").drop("code",axis=1)
df2.loc[df2.org_id.str.len()==3,"ccg_id"] =df2.org_id
df2.head()

# map CCGs onto joint teams
df3 = df2.reset_index()
df3 = ccgs.merge(df3, left_on="pct_id",right_on="ccg_id",how="left")
df3.head()


# +
GBQ_PROJECT_ID = '620265099307'

# import CCG population sizes
p = '''select pct_id, sum(total_list_size) as list_size
from `hscic.practice_statistics` as stats 
where CAST(month AS DATE) = '2018-08-01'
group by pct_id
'''

pop = pd.read_gbq(p, GBQ_PROJECT_ID, dialect='standard',verbose=False)

# merge ccgs with population data
p2 = ccgs.merge(pop, on="pct_id",how="left")

# group up to joint teams
p2 = p2.groupby("joint_id").sum().reset_index()

p2.head()

# +
# import dates of interventions
dates = pd.read_csv('allocated_ccgs_visit_timetable.csv')
dates["date"] = pd.to_datetime(dates.date)
#merge with ccgs/joint teams
dts = ccgs.merge(dates, on="joint_id",how="left").drop("pct_id",axis=1).drop_duplicates()

# merge dates with list sizes 
dts = dts.merge(p2, on="joint_id")
dts["size_rank"] = dts.groupby("allocation").list_size.rank()

#assign dummy intervention dates to control practices by pairing on total list size 
i_group = dts[["allocation","date","size_rank"]].loc[dts.allocation=="I"].drop("allocation",axis=1)

dts = dts.merge(i_group, on= "size_rank", how="left", suffixes=["","_int"]).drop("date",axis=1).sort_values(by=["size_rank","allocation"])
dts.head()

# +
# join allocated CCGs and visit dates to page views data
m = dts.drop("size_rank",axis=1).merge(df3.drop(["allocation","pct_id","ccg_id","index"],axis=1), how='left', on='joint_id')

m.head(9)

# +
# assign each page view occurrence to before vs after intervention (1 month ~ 28 days)

m["datediff"] = m.date-m.date_int
m["timing"] = "none"
m.loc[(m.datediff<="28 days")&(m.datediff> "0 days"),"timing"] = "after"
m.loc[(m.datediff>="-28 days")&(m.datediff< "0 days"),"timing"] = "before"
m["Unique Pageviews"] =m["Unique Pageviews"].fillna(0)
m.head()

# +
# group up page views data to joint teams and sum page views before and after interventions

m2 = m.loc[m.timing!=""].groupby(["allocation","joint_id","org_type","list_size","timing"]).agg({"Unique Pageviews":sum,
                                                                                      "Page":"nunique"}).unstack().fillna(0)
m2 = m2.rename(columns={"Page":"No_of_Pages"}).reset_index()
#flatten columns and drop superfluous columns
m2.columns = m2.columns.map('_'.join)
m2 = m2.drop(["Unique Pageviews_none","No_of_Pages_none"], axis=1)
m2.head()
# -

# # Engagement outcome E1 #######################################################
# ## Number of page views over one month on CCG pages showing low-priority measures, before vs after intervention, between intervention and control groups. 
#

# filter CCG page views only:
m3 = m2.loc[m2.org_type_ == "ccg"]
m3.head()

# +
# max-out top 5% to reduce any extreme outliers

mx = m3.copy()

max_out = mx['Unique Pageviews_before'].quantile(0.95)
m3["proxy_pageviews_before"] = np.where(m3['Unique Pageviews_before']<max_out, m3['Unique Pageviews_before'], max_out)

max_out_b = mx['Unique Pageviews_after'].quantile(0.95)
m3["proxy_pageviews_after"] = np.where(m3['Unique Pageviews_after']<max_out_b, m3['Unique Pageviews_after'], max_out_b)



result = pd.DataFrame({'Unique Pageviews_after': m3["Unique Pageviews_after"].describe(),
                       'Unique Pageviews_before': m3["Unique Pageviews_before"].describe(),
                       'Proxy_pageviews_after': m3["proxy_pageviews_after"].describe(),
                       'Proxy_pageviews_before': m3["proxy_pageviews_before"].describe()
                      })

result


# +
#visualise data and proxy data

import matplotlib.pyplot as plt

m3[["proxy_pageviews_after","proxy_pageviews_before","Unique Pageviews_after","Unique Pageviews_before"]].hist(bins=10)
plt.show()


# +
m4 = m3.groupby(["allocation_"])['proxy_pageviews_before','proxy_pageviews_after'].mean()

m4
# -

# ### Statistical analysis

# +

import statsmodels.formula.api as smf
data = m3.copy()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["proxy_pageviews_after"] ~ data["proxy_pageviews_before"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor').reset_index()
# -

# confidence intervals
lm.conf_int().loc["intervention"]

# # Engagement outcome E2
# ## Number of page views over one month on practice pages showing low-priority measures, before vs after intervention, grouped up to CCGs, between intervention and control groups. 

# filter practice page views only:
m5 = m2.loc[m2.org_type_ == "practice"]
m5.head()

# +
# max-out top 5% to reduce extreme outliers

mx = m5.copy()

max_out = mx['Unique Pageviews_before'].quantile(0.95)
m5["proxy_pageviews_before"] = np.where(m5['Unique Pageviews_before']<max_out, m5['Unique Pageviews_before'], max_out)

max_out_b = mx['Unique Pageviews_after'].quantile(0.95)
m5["proxy_pageviews_after"] = np.where(m5['Unique Pageviews_after']<max_out_b, m5['Unique Pageviews_after'], max_out_b)

result = pd.DataFrame({'Unique Pageviews_after': m5["Unique Pageviews_after"].describe(),
                       'Unique Pageviews_before': m5["Unique Pageviews_before"].describe(),
                       'Proxy_pageviews_after': m5["proxy_pageviews_after"].describe(),
                       'Proxy_pageviews_before': m5["proxy_pageviews_before"].describe()
                      })

result


# +
import matplotlib.pyplot as plt

m5[["proxy_pageviews_after","proxy_pageviews_before","Unique Pageviews_after","Unique Pageviews_before"]].hist(bins=10)
plt.show()

# -

m5.groupby(["allocation_"])['proxy_pageviews_before','proxy_pageviews_after'].mean()


# ### Statistical analysis

# +

import statsmodels.formula.api as smf
data = m5
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["proxy_pageviews_after"] ~ data["proxy_pageviews_before"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
# -

#confidence intervals
lm.conf_int().loc["intervention"]

# # Engagement outcomes E3 and E4 : Alert sign-ups
# ## E3 Number of registrations to OpenPrescribing CCG email alerts 
# ## E4 Number of registrations to OpenPrescribing Practice email alerts grouped up to CCG
# (New sign-ups within 3 months of intervention. The CCG registered population and number of sign-ups prior to the intervention will be co-variables.)

# +
#import data from django administration, filtered for confirmed sign-ups only (no date filter)

alerts = pd.read_csv('OrgBookmark-2018-11-02.csv')
alerts["created_at"] = pd.to_datetime(alerts.created_at)
alerts.head()
# -

# map practices to joint teams (only included randomised CCGs)
a2 = alerts.merge(map2[["ccg_id","code"]], left_on="practice",right_on="code", how="left").drop("code",axis=1)
a2.ccg_id = a2.ccg_id.combine_first(a2.pct)
a2.head()

# merge ccgs with data
a3 = a2.copy()
a3 = ccgs.merge(a3, left_on="pct_id",right_on="ccg_id",how="left")
a3.head()


# join to visit dates
a4 = dts.drop(["size_rank","allocation"],axis=1).merge(a3.drop(["approved"],axis=1), how='left', on='joint_id')
a4.head()

# +
# assign each page view occurrence to before vs after intervention (1 month ~ 28 days)
a5 = a4.copy()
a5["datediff"] = a5.created_at-a5.date_int
a5["timing"] = "none"
# all alerts set up prior to day of intervention will be used as a co-variable:
a5.loc[(a5.datediff< "0 days"),"timing"] = "before"
# main outcome: alerts set up within 3 months of intervention:
a5.loc[(a5.datediff>= "0 days")& (a5.datediff<= "54 days"),"timing"] = "after"  #(within 3 months)

# flag whether each alert is a practice or CCG alert
conditions = [
    (a5.pct.str.len()==3),
    (a5.practice.str.len()==6)]

choices = ['ccg', 'practice']
a5['org_type'] = np.select(conditions, choices, default='none')
a5.head()

# +
# aggregate data: sum alerts before and after intervention for each joint team

a6 = a5.groupby(["allocation","joint_id","list_size","timing","org_type"]).agg({"user":"nunique"}).unstack().fillna(0)
a6 = a6.rename(columns={"user":"alerts"}).unstack().reset_index().fillna(0)
#flatten columns:
a6.columns = a6.columns.map('_'.join)

a6["list_size_100k"] = a6["list_size__"]/100000
a6 = a6[["allocation__","joint_id__","list_size_100k","alerts_ccg_after","alerts_ccg_before","alerts_practice_after","alerts_practice_before"]]
a6 = a6.rename(columns={"allocation__":"allocation","joint_id__":"joint_id"})

a6.head()
# -

# summary data
a6.groupby("allocation").mean()

# ### E3: CCG alert sign-ups

# +
import statsmodels.formula.api as smf
data = a6.copy()

# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})

lm = smf.ols(formula='data["alerts_ccg_after"] ~ data["alerts_ccg_before"]+ data["list_size_100k"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
# -

# confidence intervals
lm.conf_int().loc["intervention"]

# ### E4: practice alert sign-ups

# +
import statsmodels.formula.api as smf
data = a6.copy()

# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation.map({'con':0, 'I':1})

lm = smf.ols(formula='data["alerts_practice_after"] ~ data["alerts_practice_before"] + data["list_size_100k"] + intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
# -

# confidence intervals
lm.conf_int().loc["intervention"]


