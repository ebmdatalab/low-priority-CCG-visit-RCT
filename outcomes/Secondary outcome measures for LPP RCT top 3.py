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

# # Secondary Outcomes 
# **S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.**
#
# **S2. Total items prescribed per 1000 registered patients for Co-proxamol.**
#  
# **S3. Total items prescribed per 1000 registered patients for Dosulepin.**

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

# -

# ## S1. Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined. 

# +
# Load costs and items data for each of the individual low-priority measures

#costs:
q2 = '''SELECT _TABLE_SUFFIX AS measure, pct_id, month, sum(numerator) AS cost, sum(denominator) AS denominator FROM
  `ebmdatalab.alex.cost_*`
  WHERE _TABLE_SUFFIX <> 'all_low_priority'
  AND _TABLE_SUFFIX <> 'zomnibus'
  AND month >= '2017-01-01'
  GROUP BY measure, pct_id, month
   '''
#items:
q3 = '''SELECT _TABLE_SUFFIX AS measure, pct_id, month, sum(numerator) AS items FROM
  `ebmdatalab.alex.items_*`
  WHERE _TABLE_SUFFIX <> 'all_low_priority'
  AND _TABLE_SUFFIX <> 'zomnibus'
  AND month >= '2017-01-01'
  GROUP BY measure, pct_id, month'''

lpcosts = pd.read_gbq(q2, GBQ_PROJECT_ID, dialect='standard',verbose=False)
lpitems = pd.read_gbq(q3, GBQ_PROJECT_ID, dialect='standard',verbose=False)

lpcosts["month"] = pd.to_datetime(lpcosts.month)
lpitems["month"] = pd.to_datetime(lpitems.month)

lpcosts.head() # this gives the first few rows of data

# -

# merge items and cost into a single table
lp = lpcosts.merge(lpitems, on=["measure","pct_id","month"], how="outer")
lp.head()

# +
### select data only for the baseline and follow-up periods
import datetime

conditions = [
    (lp['month']  >= d4), # after follow-up period
    (lp['month']  >= d3), # follow-up
    (lp['month']  >= d2), # mid
    (lp['month']  >= d1), # baseline
    (lp['month']  < d1)] # before

choices = ['after', 'follow-up', 'mid', 'baseline','before']
lp['period'] = np.select(conditions, choices, default='0')

# take columns of interest from df
df2 = lp[["measure","pct_id","period", "month", "cost","items","denominator"]]
df2 = df2.loc[(df2['period']== "baseline") | (df2['period']== "follow-up")].set_index(["pct_id","period", "month"])
df2.head()
# -

### sum numerator and average population denominators for each CCG for each period
agg_6m = df2.groupby(["measure","pct_id","period"]).agg({"cost":sum,"items":sum,"denominator":"mean"})
agg_6m.head()

# +
### import **allocated** CCGs
ccgs = pd.read_csv('randomisation_group.csv')
# import joint team information
team = pd.read_csv('joint_teams.csv')

ccgs = ccgs.merge(team,on="joint_team", how="left")
#fill black ccg_ids from joint_id column
ccgs["pct_id"] = ccgs["ccg_id"].combine_first(ccgs["joint_id"])
ccgs = ccgs[["joint_id","allocation","pct_id"]]

df2b = agg_6m.reset_index()
df2b = ccgs.merge(df2b, on="pct_id",how="left")
df2b.head()


# +
# group up to Joint team groups 
# note: SUM both numerators and population denominator across geographies
df2c = df2b.groupby(["joint_id","allocation","measure","period"]).sum()
df2c = df2c.unstack().reset_index()
df2c.columns = df2c.columns.map('_'.join)

### calculate aggregated measure values (cost only)
df2c["baseline_calc_value"] = df2c.cost_baseline / df2c.denominator_baseline
df2c["follow_up_calc_value"] = df2c["cost_follow-up"] / df2c["denominator_follow-up"]

df2c.head()

# -

# find top 3 measures per CCG by cost
df3 = df2c.sort_values(by=["joint_id_","baseline_calc_value"], ascending=False)
df3["measure_rank"] = df3.groupby("joint_id_")["baseline_calc_value"].rank(ascending=False)
df4 = df3.loc[df3.measure_rank <=3]
df4.head()

# +
df5 = df4.copy()
df5 = df5.groupby(["joint_id_","allocation_"]).agg({"cost_baseline":"sum","cost_follow-up":"sum","denominator_baseline":"mean","denominator_follow-up":"mean"})

### calculate aggregated measure values for combined cost for the top 3 measures
df5["baseline_calc_value"] = df5.cost_baseline / df5.denominator_baseline
df5["follow_up_calc_value"] = df5["cost_follow-up"] / df5["denominator_follow-up"]
df5.head() 


# +
# secondary outcome: Cost per 1,000 patients for top 3 pre-specified “low-priority” treatments combined.

import statsmodels.formula.api as smf
data = df5.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
# -

# ## S2: Total items prescribed per 1000 registered patients for Co-proxamol. 

# +
# filter data for coproxamol measure:
df6 = df2c.copy()
df6 = df6.loc[df6.measure_=="coprox"]

### calculate aggregated measure values (items per 1000 patients)
df6["baseline_calc_value"] = df6.items_baseline / df6.denominator_baseline
df6["follow_up_calc_value"] = df6["items_follow-up"] / df6["denominator_follow-up"]
df6.head()

# +
## Secondary outcome: Total items prescribed per 1000 registered patients for Co-proxamol.
import statsmodels.formula.api as smf
data = df6.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
# -

# ## S3: Total items prescribed per 1000 registered patients for Dosulepin. 

# +
# filter data for dosulepin measure:
df7 = df2c.copy()
df7 = df7.loc[df7.measure_=="dosulepin"]

### calculate aggregated measure values (items per 1000 patients)
df7["baseline_calc_value"] = df7.items_baseline / df7.denominator_baseline
df7["follow_up_calc_value"] = df7["items_follow-up"] / df7["denominator_follow-up"]
df7.head()

# +
## Secondary outcome: Total items prescribed per 1000 registered patients for Dosulepin.
import statsmodels.formula.api as smf
data = df7.copy().reset_index()
# create a new Series called "intervention" to convert intervention/control to numerical values
data['intervention'] = data.allocation_.map({'con':0, 'I':1})

lm = smf.ols(formula='data["follow_up_calc_value"] ~ data["baseline_calc_value"] +intervention', data=data).fit()

#output regression coefficients and p-values:
params = pd.DataFrame(lm.params).reset_index().rename(columns={0: 'coefficient','index': 'factor'})
pvals = pd.DataFrame(lm.pvalues[[1,2]]).reset_index().rename(columns={0: 'p value','index': 'factor'})
params.merge(pvals, how='left',on='factor').set_index('factor')
