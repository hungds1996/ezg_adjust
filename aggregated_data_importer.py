from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging
import datetime
import argparse
import smtplib, ssl
import traceback
import requests as rq
import sys

from config import *

logging.basicConfig(
    level=logging.INFO,
    filename="./logs/adjust.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
        raise Exception(
            "Start date ({}) must be smaller than end date ({})".format(
                start_date, end_date
            )
        )

    return start_date, end_date


def pull_adjust_data(start_date, end_date):
    # api_url = 'https://api.adjust.com/kpis/v1/{APP_TOKENS}.csv?kpis={KPIS}&grouping={GROUPBY}&start_date={START_DATE}&end_date={END_DATE}&user_token={USER_TOKEN}'.format(
    #     APP_TOKENS=",".join(apps_token),
    #     KPIS=",".join(kpis),
    #     GROUPBY=groupby,
    #     START_DATE=start_date,
    #     END_DATE=end_date,
    #     USER_TOKEN=api_token
    # )
    base_url = "https://dash.adjust.com/control-center/reports-service/report?ad_spend_mode=network&app_token__in={app_tokens}&date_period={date_period}&dimensions={dimensions}&metrics={metrics}"
    api_url = base_url.format(
        app_tokens=",".join(apps_token),
        date_period=start_date + ":" + end_date,
        dimensions=groupby,
        metrics=kpis,
    )

    # response = pd.read_csv(api_url)
    response = rq.get(
        api_url,
        headers={"Authorization": "Bearer " + api_token},
    ).json()

    if "error_code" in response:
        raise Exception("Adjust API ERROR: {}".format(response["error_desc"]))

    df = pd.DataFrame(response["rows"])
    if len(df) == 0:
        raise Exception("Adjust API ERROR: No data returned")
    df = df.drop(["attr_dependency"], axis=1)

    return df


def transform_data(response, app):
    """Transform data to match with bigquery schema

    Args:
        response (pandas DataFrame): Dataframe from adjust API
        app (String): app token

    Returns:
        pandas DataFrame: Dataframe with app_id column
    """
    this_app_token = app["info"]["app_token"]
    df_to_write = response[response["app_token"] == this_app_token]
    df_to_write["app_id"] = app["info"]["app_id"]
    df_to_write["updated_date"] = datetime.datetime.now().strftime("%Y-%m-%d")
    df_to_write = df_to_write[
        [
            "campaign",
            "network",
            "app_token",
            "country_code",
            "adgroup",
            "country",
            "app",
            "partner_name",
            "adgroup_network",
            "day",
            "store_id",
            "store_type",
            "source_network",
            "installs",
            "network_installs",
            "clicks",
            "network_clicks",
            "impressions",
            "network_impressions",
            "organic_installs",
            "cost",
            "adjust_cost",
            "network_cost",
            "click_cost",
            "install_cost",
            "impression_cost",
            "ad_revenue",
            "attribution_clicks",
            "app_id",
            "updated_date",
        ]
    ]

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

        logger.debug(
            "{} rows + to file + {}".format(df_to_write.shape[0], app["info"]["app_id"])
        )

        total_rows += csv_to_bigquery(app=app)
    return total_rows


def csv_to_bigquery(app):
    table_id = app["info"]["table_id"]
    table = get_or_create_table(table_id)

    row_before = table.num_rows

    job_config, _ = make_job_config()
    with open(temp_file, mode="rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()

    row_after = client.get_table(table_id).num_rows
    row_imported = row_after - row_before
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
            type_=bigquery.TimePartitioningType.DAY, field="date", expiration_ms=None
        )
        table = client.create_table(table)
        logger.info(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        return client.get_table(table_id)


def make_job_config():
    """Declare schema for bigquery table and make job config"""
    schema = [
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("network", "STRING"),
        bigquery.SchemaField("app_token", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("adgroup", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("app", "STRING"),
        bigquery.SchemaField("partner_name", "STRING"),
        bigquery.SchemaField("adgroup_network", "STRING"),
        bigquery.SchemaField("day", "DATE"),
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_type", "STRING"),
        bigquery.SchemaField("source_network", "STRING"),
        bigquery.SchemaField("installs", "FLOAT"),
        bigquery.SchemaField("network_installs", "FLOAT"),
        bigquery.SchemaField("clicks", "FLOAT"),
        bigquery.SchemaField("network_clicks", "FLOAT"),
        bigquery.SchemaField("impressions", "FLOAT"),
        bigquery.SchemaField("network_impressions", "FLOAT"),
        bigquery.SchemaField("organic_installs", "FLOAT"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("adjust_cost", "FLOAT"),
        bigquery.SchemaField("network_cost", "FLOAT"),
        bigquery.SchemaField("click_cost", "FLOAT"),
        bigquery.SchemaField("install_cost", "FLOAT"),
        bigquery.SchemaField("impression_cost", "FLOAT"),
        bigquery.SchemaField("ad_revenue", "FLOAT"),
        bigquery.SchemaField("attribution_clicks", "STRING"),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("updated_date", "DATE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema, skip_leading_rows=1, write_disposition="WRITE_APPEND"
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
