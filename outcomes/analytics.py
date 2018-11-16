"""
Turn a Google Analytics v4 API query to a dataframe.

API documentation:
https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet

List of metrics and dimensions:
https://developers.google.com/analytics/devguides/reporting/core/dimsmets
"""

from apiclient.discovery import build
import pandas as pd
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
# Path to client_secrets.json file.
CLIENT_SECRETS_PATH = 'client_secrets.json'


def initialize_analyticsreporting():
    """Initializes the analyticsreporting service object.

    Returns: an authorized analyticsreporting service
       object.

    """
    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        CLIENT_SECRETS_PATH, scope=SCOPES,
        message=tools.message_if_missing(CLIENT_SECRETS_PATH))

    # Prepare credentials, and authorize HTTP object with them.  If
    # the credentials don't exist or are invalid run through the
    # native client flow. The Storage object will ensure that if
    # successful the good credentials will get written back to a file.
    storage = file.Storage('analyticsreporting.dat')
    credentials = storage.get()
    flags = []
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())
    # Build the service object.
    analytics = build('analytics', 'v4',
                      http=http,
                      discoveryServiceUrl=DISCOVERY_URI)
    return analytics


def get_report(analytics, query):
    # Use the Analytics Service Object to query the Analytics Reporting API V4.
    return analytics.reports().batchGet(
        body={
            'reportRequests': query
        }
    ).execute()


def query_analytics(query, columns=[]):
    """Parses and prints the Analytics Reporting API V4 response"""
    analytics = initialize_analyticsreporting()
    response = get_report(analytics, query)
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        samplesReadCounts = report.get('data', {}).get('samplesReadCounts', [])
        samplingSpaceSizes = report.get('data', {}).get('samplingSpaceSizes', [])
    if samplesReadCounts:
        print("Warning: data sampled at around {}%".format(
            round(float(samplesReadCounts[0])/samplingSpaceSizes[0] * 100)))
    data = []
    for row in rows:
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])
        data_row = {}

        for header, dimension in zip(dimensionHeaders, dimensions):
            data_row[header] = dimension

        for i, values in enumerate(dateRangeValues):
            for metricHeader, value in zip(metricHeaders, values.get('values')):
                col_name = metricHeader.get('name')
                if len(dateRangeValues) > 1:
                    col_name += "_range_{}".format(i)
                if value:
                    if "." in value:
                        value = float(value)
                    else:
                        value = int(value)
                data_row[col_name] = value
                data.append(data_row)
    df = pd.DataFrame(data)
    for col in df.columns:
        if col.startswith("ga:date"):
            df[col] = pd.to_datetime(df[col])
    if columns:
        df.columns = columns
    return df
