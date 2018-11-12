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

# # Primary outcomes
# **P1.  Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined.**
#
# **P2. Total items per 1000 across all 18 low priority treatments.**
#

# +
# Set dates of baseline and follow-up periods
d4 = '2018-07-01' # month after end of follow-up period
d3 = '2018-01-01' # follow-up start
d2 = '2017-07-01' # month after end of baseline period
d1 = '2017-01-01' # baseline start

# Import dataset from BigQuery
import pandas as pd
import numpy as np
GBQ_PROJECT_ID = '620265099307'

# costs (totals, not divided into individual measures) - for P1
q = '''SELECT * FROM ebmdatalab.measures.ccg_data_lpzomnibus
WHERE EXTRACT (YEAR from month)  >= 2017
'''

# items (summed across all lp measures) - for P2:
q3 = '''SELECT pct_id, month, sum(numerator) AS items FROM
  `ebmdatalab.alex.items_*`
  WHERE _TABLE_SUFFIX <> 'all_low_priority'
  AND _TABLE_SUFFIX <> 'zomnibus'
  AND month >= '2017-01-01'
  GROUP BY pct_id, month'''

df1 = pd.read_gbq(q, GBQ_PROJECT_ID, dialect='standard',verbose=False)
df1["month"] = pd.to_datetime(df1.month)

lpitems = pd.read_gbq(q3, GBQ_PROJECT_ID, dialect='standard',verbose=False)
lpitems["month"] = pd.to_datetime(lpitems.month)

df1.head() # this gives the first few rows of data
# -

# merge items and costs data
df1a = df1.merge(lpitems, on=["pct_id","month"],how="outer").sort_values(by=["pct_id","month"])
df1a.head()

# +
### select data only for the baseline and follow-up periods
import datetime

conditions = [
    (df1a['month']  >= d4), # after follow-up period
    (df1a['month']  >= d3), # follow-up
    (df1a['month']  >= d2), # mid
    (df1a['month']  >= d1), # baseline
    (df1a['month']  < d1)] # before

choices = ['after', 'follow-up', 'mid', 'baseline','before']
df1a['period'] = np.select(conditions, choices, default='0')

# take columns of interest from df
df2 = df1a[["pct_id","period", "month", "numerator","denominator","items"]]
df2 = df2.loc[(df2['period']== "baseline") | (df2['period']== "follow-up")].set_index(["pct_id","period", "month"])
df2.head(10)
# -

### sum numerators for each CCG for each period
agg_6m = df2.groupby(["pct_id","period"]).agg({"numerator":sum,"items":sum,"denominator":"mean"})
agg_6m.head()

# +
### import **allocated** CCGs
ccgs = pd.read_csv('../data/randomisation_group.csv')
# import joint team information
team = pd.read_csv('../data/joint_teams.csv')

ccgs = ccgs.merge(team,on="joint_team", how="left")
#fill black ccg_ids from joint_id column
ccgs["pct_id"] = ccgs["ccg_id"].combine_first(ccgs["joint_id"])
ccgs = ccgs[["joint_id","allocation","pct_id"]]

# merge ccgs with data
df2b = agg_6m.reset_index()
df2b = ccgs.merge(df2b, on="pct_id",how="left")
df2b.head()

# -

# group up to Joint team groups
# note: SUM both numerator and population denominator across geographies
df2c = df2b.groupby(["joint_id","allocation","period"]).sum()
df2c = df2c.unstack().reset_index()
df2c.columns = df2c.columns.map('_'.join)
df2c.head()

# +
# calculate aggregated measure values
df2c["baseline_calc_value"] = df2c.numerator_baseline / df2c.denominator_baseline
df2c["follow_up_calc_value"] = df2c["numerator_follow-up"] / df2c["denominator_follow-up"]

df2c["baseline_items_thou"] = df2c.items_baseline / df2c.denominator_baseline
df2c["follow_up_items_thou"] = df2c["items_follow-up"] / df2c["denominator_follow-up"]

df2c.head()

# +
# plot time series chart for intervention versus control

'''# merge MONTHLY data with practice allocations
dfp = df1.loc[df1.month_no>0]
dfp = dfp.loc[~pd.isnull(dfp.calc_value)]
dfp = prac.merge(dfp, how='left', on='practice_id')#.set_index('allocation')
dfp = dfp[['practice_id','month_no','allocation','calc_value']]

dfp2 = dfp.groupby(['month_no','allocation']).count()
#dfp = pd.DataFrame(dfp.to_records())#.set_index('month_no')
dfp2

import seaborn as sns#; sns.set(color_codes=True)
import matplotlib.pyplot as plt
sns.set(style="darkgrid")

g = sns.tsplot(data=dfp, time="month_no",  value="calc_value", unit="practice_id",condition="allocation")
plt.ylim((0, 0.17))
plt.show()'''


# +
### Primary Outcome ########################
# Cost per 1,000 patients for all 18 pre-specified “low-priority” treatments combined,
# between intervention and control groups, assessed by applying a multivariable linear regression model.

import statsmodels.formula.api as smf
data = df2c
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')


# +
### Primary Outcome P2 ########################
# ITEMS per 1,000 patients for all 18 pre-specified “low-priority” treatments combined,
# between intervention and control groups, assessed by applying a multivariable linear regression model.

import statsmodels.formula.api as smf
data = df2c
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["follow_up_items_thou"] ~ data["baseline_items_thou"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')

# -

# # remaining:
# ## add confidence intervals
