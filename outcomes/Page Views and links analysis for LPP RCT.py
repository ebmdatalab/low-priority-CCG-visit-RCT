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

# Imports and variables
from IPython.display import display
import pandas as pd
import numpy as np
import analytics
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf

DUMMY_RUN = True  # Change this to False when the analysis is run for real
ANALYTICS_VIEW_ID = '101677264'
GBQ_PROJECT_ID = '620265099307'

# +
# Import page views data
#
# # Engagement outcomes
#
# For the primary and some secondary outcomes, we need to use Google Analytics page views data:
#
# ## E1: Number of page views over one month on CCG page showing low-priority measures
# ## E2: Number of page views over one month on practice pages showing low-priority measures, grouped up to CCGs
#
# Timepoints:
# - 1 month before/after
# - April-Sept 2018 vs April-Sept 2019
#

if DUMMY_RUN:
    # CCG-level data:
    ccg_stats = pd.read_csv('../data/page_views_dummy_ccg.csv',usecols={"Page","Date","Pageviews","Unique Pageviews"} )
    # practice-level data:
    practice_stats = pd.read_csv('../data/page_views_dummy_practice.csv',usecols={"Page","Date","Pageviews","Unique Pageviews"} )
    ccg_stats['date'] = pd.to_datetime(ccg_stats.Date, format="%Y%m%d")
    practice_stats['date'] = pd.to_datetime(practice_stats.Date, format="%Y%m%d")
    # Filter out wrongly included lines in dummy data
    ccg_stats = ccg_stats[ccg_stats.Page.str.contains("lowpriority")]
    practice_stats = practice_stats[practice_stats.Page.str.contains("lowpriority")]
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
                        "expressions": ["^/ccg.*lowp"]
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
    ccg_stats = analytics.query_analytics(ccg_query, columns=colnames)

    # ...and the same query at practice level
    practice_query = ccg_query.copy()
    practice_query[0]["dimensionFilterClauses"][0]["filters"][0]["expressions"] = ["^/practice.*lowp"]
    practice_stats = analytics.query_analytics(practice_query, columns=colnames)

    ccg_stats.to_csv("../data/ccg_pageview_stats.csv")
    practice_stats.to_csv("../data/practice_pageview_stats.csv")
# -

ccg_stats.head()

all_stats = pd.concat([ccg_stats,practice_stats], sort=False)

# extract ccg/practice code from path
all_stats["org_id"] = np.where(
    all_stats.Page.str.contains("ccg"),
    all_stats.Page.str.replace('/ccg/', '').str[:3],
    all_stats.Page.str.replace('/practice/', '').str[:6])
all_stats["org_type"] = np.where(
    all_stats.Page.str.contains("ccg"),
    "ccg",
    'practice')
all_stats.head()

# +
### import allocated Rct_Ccgs
rct_ccgs = pd.read_csv('../data/randomisation_group.csv')

# joint team information
team = pd.read_csv('../data/joint_teams.csv')

# create map of rct_ccgs to joint teams
rct_ccgs = rct_ccgs.merge(team, on="joint_team", how="left")

# fill blank ccg_ids from joint_id column, so every CCG has a value for joint_id
rct_ccgs["pct_id"] = rct_ccgs["ccg_id"].combine_first(rct_ccgs["joint_id"])
rct_ccgs = rct_ccgs[["joint_id", "allocation", "pct_id"]]
rct_ccgs.head()

# +
## Map practices to Rct_Ccgs, for practice-level analysis

# Get current mapping data from bigquery
practice_to_ccg = '''select distinct ccg_id, code
from `ebmdatalab.hscic.practices`
where setting = 4 and status_code != 'C'
'''

practice_to_ccg = pd.read_gbq(practice_to_ccg, GBQ_PROJECT_ID, dialect='standard', verbose=False)
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
pop = pd.read_gbq(query, GBQ_PROJECT_ID, dialect='standard', verbose=False)
pop.to_csv("../data/practice_statistics.csv")

# +
# merge rct_ccgs with population data
ccg_populations = rct_ccgs.merge(pop, on="pct_id", how="left")

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
#allocations_with_dates_and_sizes[((allocations_with_dates_and_sizes['joint_id'] == '02G') & (allocations_with_dates_and_sizes['date_int'] == '2018-10-05'))]
# -

# join joint-group / ccg allocations, visit dates and list size info to page views data
all_data = allocations_with_dates_and_sizes.drop("size_rank", axis=1)\
       .merge(
           stats_with_allocations.drop(["allocation", "pct_id", "ccg_id"], axis=1),
           how='left',
           on='joint_id')
all_data.head(2)

# +
# assign each page view occurrence to before vs after intervention (1 month ~ 28 days)

all_data["datediff"] = all_data.date - all_data.date_int
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

all_data_agg = all_data.groupby(["allocation", "joint_id", "org_type", "list_size", "timing"])\
      .agg({"Unique Pageviews": sum, "Page": "nunique"}).unstack().fillna(0)
all_data_agg = all_data_agg.rename(columns={"Page": "No_of_Pages"}).reset_index()
#flatten columns and drop superfluous columns
all_data_agg.columns = all_data_agg.columns.map('_'.join)
all_data_agg = all_data_agg.drop(["Unique Pageviews_none","No_of_Pages_none"], axis=1)
all_data_agg.head()
# -

all_data_agg[((all_data_agg['joint_id_'] == '01V'))]

# # Engagement outcome E1 #######################################################
# ## Number of page views over one month on CCG pages showing low-priority measures, before vs after intervention, between intervention and control groups.
#

# +
def trim_5_percentiles(df, debug=False):
    # max-out top 5% to reduce any extreme outliers
    df = df.copy()

    max_out = df['Unique Pageviews_before'].quantile(0.95)
    df["proxy_pageviews_before"] = np.where(
        df['Unique Pageviews_before'] < max_out,
        df['Unique Pageviews_before'],
        max_out)
    max_out_b = df['Unique Pageviews_after'].quantile(0.95)
    df["proxy_pageviews_after"] = np.where(
        df['Unique Pageviews_after'] < max_out_b,
        df['Unique Pageviews_after'],
        max_out_b)

    if debug: 
        result = pd.DataFrame(
            {'Unique Pageviews_after': df["Unique Pageviews_after"].describe(),
             'Unique Pageviews_before': df["Unique Pageviews_before"].describe(),
             'Proxy_pageviews_after': df["proxy_pageviews_after"].describe(),
             'Proxy_pageviews_before': df["proxy_pageviews_before"].describe()})
        df[["proxy_pageviews_after",
                "proxy_pageviews_before",
            "Unique Pageviews_after",
            "Unique Pageviews_before"]].hist(bins=10)
        display("Descriptive stats:")
        display(result)
        display("Histogram before and after trimming:")
        plt.show()
        display("Mean pageviews before and after:")
        display(ccg_data_agg_trimmed.groupby(["allocation_"])[
        'proxy_pageviews_before', 'proxy_pageviews_after'].mean())
    return df


def compute_regression(df):
    data = df.copy()

    # create a new Series called "intervention" to convert
    # intervention/control to numerical values
    data['intervention'] = data.allocation_.map({'con': 0, 'I': 1})

    lm = smf.ols(
        formula=('data["proxy_pageviews_after"] ~ data["proxy_pageviews_before"] '
                 '+intervention'),
        data=data).fit()

    # output regression coefficients and p-values:
    params = pd.DataFrame(lm.params).reset_index().rename(
        columns={0: 'coefficient', 'index': 'factor'})
    pvals = pd.DataFrame(lm.pvalues[[1, 2]]).reset_index().rename(
        columns={0: 'p value', 'index': 'factor'})
    params = params.merge(pvals, how='left', on='factor').set_index('factor')

    # add confidence intervals
    conf = pd.DataFrame(data=lm.conf_int())
    conf.columns = ["conf_int_low", "conf_int_high"]
    return params.join(conf)
# -

# filter CCG page views only:
ccg_data_agg = all_data_agg.loc[all_data_agg.org_type_ == "ccg"]
ccg_data_agg_trimmed = trim_5_percentiles(ccg_data_agg, debug=False)
compute_regression(ccg_data_agg_trimmed)

# +
# # Engagement outcome E2

practice_data_agg = all_data_agg.loc[all_data_agg.org_type_ == "practice"]
practice_data_agg_trimmed = trim_5_percentiles(practice_data_agg, debug=False)
compute_regression(practice_data_agg_trimmed)
# -


# # Engagement outcomes E3 and E4 : Alert sign-ups
# ## E3 Number of registrations to OpenPrescribing CCG email alerts
# ## E4 Number of registrations to OpenPrescribing Practice email alerts grouped up to CCG
# (New sign-ups within 3 months of intervention. The CCG registered population and number of sign-ups prior to the intervention will be co-variables.)

# +
#import data from django administration, filtered for confirmed sign-ups only (no date filter)

alerts = pd.read_csv('../data/OrgBookmark-2018-11-02.csv')
alerts["created_at"] = pd.to_datetime(alerts.created_at)
alerts.head()
# -

# map practices to joint teams (only included randomised Rct_Ccgs)
a2 = alerts.merge(map2[["ccg_id","code"]], left_on="practice",right_on="code", how="left").drop("code",axis=1)
a2.ccg_id = a2.ccg_id.combine_first(a2.pct)
a2.head()

# merge rct_ccgs with data
a3 = a2.copy()
a3 = rct_ccgs.merge(a3, left_on="pct_id",right_on="ccg_id",how="left")
a3.head()


# join to visit dates
a4 = allocations_with_dates_and_stats.drop(["size_rank","allocation"],axis=1).merge(a3.drop(["approved"],axis=1), how='left', on='joint_id')
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
