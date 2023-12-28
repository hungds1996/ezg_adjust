from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, 
    filename='./logs/adjust.log', 
    filemode='w', 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_client():
    credentials = service_account.Credentials.from_service_account_file(json_keys)
    return bigquery.Client(credentials=credentials)

def pull_adjust_data(start_date, end_date, apps_token, kpis, groupby, api_token):
    api_url = 'https://api.adjust.com/kpis/v1/{APP_TOKENS}.csv?kpis={KPIS}&grouping={GROUPBY}&start_date={START_DATE}&end_date={END_DATE}&user_token={USER_TOKEN}'.format(
        APP_TOKENS=",".join(apps_token),
        KPIS=",".join(kpis),
        GROUPBY=groupby,
        START_DATE=start_date,
        END_DATE=end_date,
        USER_TOKEN=api_token
    )
    
    response = pd.read_csv(api_url)
    
    

def transform_data(response, app):
    this_app_token = app['info']['app_token']
    df_to_write = response[response['app_token']==this_app_token]
    df_to_write['app_id'] = app['info']['app_id']

    return df_to_write

def import_logger(client, end_date, rows_imported, table_id=''):
    data_to_insert = [{
        u"job": u"aggregated_data_importer",
        u"rows_imported": rows_imported,
        u"date": end_date
    }]
    
    client.insert_rows_json(table_id, data_to_insert)
    
def get_last_date(client, table_id):
    query = """
        select date_add(max(date), interval 1 day) last_date
        from `{}`
    """.format(table_id)
    
    return client.query(query).to_dataframe()['last_date'][0]

class aggregatedDataImporter:
    def __init__(self, client, app_info, bq_job_config, report_url, temp_file='adjust_temp.csv'):
        self.client = client
        self.app_info = app_info
        self.bq_job_config = bq_job_config
        self.report_url = report_url
        self.temp_file = temp_file
        self.total_rows_imported = 0
        
    def data_to_bigquery(self):
        response = pd.read_csv(self.report_url)

        for app in self.app_info:
            df_to_write = transform_data(response, app)
            df_to_write.to_csv(self.temp_file, index=False)
            
            logger.info('{}: Saved {} rows to file'.format(
                app['info']['app_id'],
                df_to_write.shape[0]
            ))
            
            self.csv_to_bigquery(app=app)
        
    def csv_to_bigquery(self, app):
        table_id = app['info']['table_id']
        
        row_before = self.client.get_table(table_id).num_rows
        
        with open(self.temp_file, 'rb') as source_file:
            job = self.client.load_table_from_file(
                source_file, 
                table_id, 
                job_config=self.bq_job_config
            )
            
            job.result()
            
        row_after = self.client.get_table(table_id).num_rows
        row_imported = row_after - row_before
        
        logger.info('{}: Imported {} rows to BigQuery table {}'.format(
            app['info']['app_id'],
            row_imported,
            table_id
        ))  