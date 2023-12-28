import datetime
import logging
import sys
import argparse

from app_info import app_info
from aggregated_data_importer import *
from config import *

def get_start_end_date():
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = get_last_date(client)
        if not start_date:
            if args.end_date:
                start_date = args.end_date
            else:
                start_date = end_date
        else:
            start_date = start_date.strftime("%Y-%m-%d")
    
    if start_date > end_date:
        logger.error('Start date ({}) is greater than end date ({})'.format(start_date, end_date))
        sys.exit()
    
    return start_date, end_date


parser=argparse.ArgumentParser()
parser.add_argument('--start_date', help='Start date for import')
parser.add_argument('--end_date', help='End date for import')
args = parser.parse_args()

# client = get_client()
start_date, end_date = get_start_end_date()

if __name__ == '__main__':
    
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND'
    )
        
        