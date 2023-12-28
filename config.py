import datetime
from google.cloud import bigquery


end_date = str(datetime.date.today() - datetime.timedelta(days=1))
json_keys = './JSON keys/speedster-hit-682166e75581.json'

api_token = 'w7CPtFhxhKKubxeAtVrf'
apps_token = ['s07u9q9vrbi8','zw62z1c7okcg','dkkx6zat0jr4','h38g236bjojk','t2b0fh0roav4']
kpis = ['install_cost', 'click_cost', 'impression_cost','cost','ad_impressions','ad_revenue','paid_installs','paid_clicks','paid_impressions','clicks', 'impressions', 'installs']
groupby='apps,days,networks,countries'

schema=[
    bigquery.SchemaField('app_token', 'STRING'),
    bigquery.SchemaField('app_name', 'STRING'),
    bigquery.SchemaField('date', 'DATE'),
    bigquery.SchemaField('tracker_token', 'STRING'),
    bigquery.SchemaField('network', 'STRING'),
    bigquery.SchemaField('country', 'STRING'),
    bigquery.SchemaField('install_cost', 'FLOAT'),
    bigquery.SchemaField('click_cost', 'FLOAT'),
    bigquery.SchemaField('impression_cost', 'FLOAT'),
    bigquery.SchemaField('cost', 'FLOAT'),
    bigquery.SchemaField('ad_impressions', 'FLOAT'),
    bigquery.SchemaField('ad_revenue', 'FLOAT'),
    bigquery.SchemaField('paid_installs', 'FLOAT'),
    bigquery.SchemaField('paid_clicks', 'FLOAT'),
    bigquery.SchemaField('paid_impressions', 'FLOAT'),
    bigquery.SchemaField('clicks', 'FLOAT'),
    bigquery.SchemaField('impressions', 'FLOAT'),
    bigquery.SchemaField('installs', 'FLOAT'),
    bigquery.SchemaField('app_id', 'STRING'),
]