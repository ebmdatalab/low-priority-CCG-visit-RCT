import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display
import statsmodels.formula.api as smf


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
        display(df\
                .groupby(["allocation"])\
                ['proxy_pageviews_before', 'proxy_pageviews_after']\
                .mean())
    return df


def compute_regression(df, formula=""):
    """Fit a regression model
    """
    # Uses R-style formula as described here
    # https://www.statsmodels.org/dev/example_formulas.html
    data = df.copy()

    lm = smf.ols(
        formula=formula,
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
