from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
import datetime
import argparse
import smtplib, ssl
import traceback
import sys

from config import *

logging.basicConfig(
    level=logging.INFO, 
    filename='./logs/adjust.log', 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_client():
    """Builds a bigquery client with the given json key file."""
    credentials = service_account.Credentials.from_service_account_file(json_keys)
    return bigquery.Client(credentials=credentials)

client = get_client()


def get_start_end_date(report_name):
    """Get start and end date from command line arguments or from last date in bigquery table

    Args:
        report_name (_type_): Name of the report

    Raises:
        Exception: Start date must be smaller than end date

    Returns:
        _type_: start_date and end_date in string format
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", help="predefined start date")
    parser.add_argument("--end_date", help="predefined end date")
    args = parser.parse_args()

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=7)
        start_date = start_date.strftime("%Y-%m-%d")

    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.date.today() - datetime.timedelta(days=2)
        end_date = end_date.strftime("%Y-%m-%d")
    
    if start_date > end_date:
        raise Exception("Start date ({}) must be smaller than end date ({})".format(start_date, end_date))
    
    return start_date, end_date

def pull_adjust_data(start_date, end_date):
    api_url = 'https://api.adjust.com/kpis/v1/{APP_TOKENS}.csv?kpis={KPIS}&grouping={GROUPBY}&start_date={START_DATE}&end_date={END_DATE}&user_token={USER_TOKEN}'.format(
        APP_TOKENS=",".join(apps_token),
        KPIS=",".join(kpis),
        GROUPBY=groupby,
        START_DATE=start_date,
        END_DATE=end_date,
        USER_TOKEN=api_token
    )
    try:
        response = pd.read_csv(api_url)
    
        return response
    except Exception as e:
        raise Exception("Adjust API ERROR: {}".format(e))
    

def transform_data(response, app):
    """Transform data to match with bigquery schema

    Args:
        response (pandas DataFrame): Dataframe from adjust API
        app (String): app token

    Returns:
        pandas DataFrame: Dataframe with app_id column
    """
    this_app_token = app['info']['app_token']
    df_to_write = response[response['app_token']==this_app_token]
    df_to_write['app_id'] = app['info']['app_id']

    return df_to_write

def import_data_bigquery(df):
    """loop through the dataframe and import data of each app to bigquery

    Args:
        df (pandas dataframe): 

    Returns:
        int: total rows imported
    """
    total_rows = 0
    for app in app_info:
        df_to_write = transform_data(df, app)
        df_to_write.to_csv(temp_file, index=False) 
        
        logger.debug('{} rows + to file + {}'.format(df_to_write.shape[0],app['info']['app_id']))
        
        total_rows += csv_to_bigquery(app=app)   
    return total_rows


def csv_to_bigquery(app):
    table_id = app['info']['table_id']
    table = get_or_create_table(table_id)
    
    row_before = table.num_rows
    
    job_config, _ = make_job_config()
    with open(temp_file, mode='rb') as source_file:
        job = client.load_table_from_file(
            source_file, 
            table_id, 
            job_config=job_config
        )

        job.result()

    row_after = client.get_table(table_id).num_rows
    row_imported = (row_after-row_before)
    logger.debug("App {}: Imported {} rows".format(app, row_imported))
    
    return row_imported
        
        
def get_or_create_table(table_id):
    """Look for the table in biquery, if not found create a new one

    Args:
        table_id (str): bigquery table name format "project.dataset.table"
    """
    try:
        return client.get_table(table_id)
    except:
        _, schema = make_job_config()
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
            expiration_ms=None
        )
        table = client.create_table(table)
        logger.info(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        return client.get_table(table_id)

def make_job_config():
    """Declare schema for bigquery table and make job config
    """
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
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND'
    )
    return job_config, schema


def import_logger(data_date, report_name, rows_imported):
    """Import log to bigquery table"""
    log_df = pd.DataFrame.from_dict(
        {
            "data_date": [data_date],
            "import_date": [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            "report_name": [report_name],
            "rows_imported": [rows_imported],
        }
    )
    log_df.to_csv(".\data\import_log.csv", index=False)
    

def send_email(receiver_email, e):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    error_message = str(e)
    traceback_message = traceback.format_exc()

    message = f"""\
    Subject: ADJUST IMPORTING ERROR

    Error content:
    Error: {error_message}\n\nTraceback: {traceback_message}"""

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(email_config["sender"], email_config["password"])
        server.sendmail(email_config["sender"], receiver_email, message)
