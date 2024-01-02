from aggregated_data_importer import *
from config import *

if __name__ == "__main__":
    try:
        start_date, end_date = get_start_end_date("adjust_aggregrated_data_daily")
        df = pull_adjust_data(start_date, end_date)
        total_rows = import_data_bigquery(df)
        import_logger(df["day"].max(), "adjust_aggregrated_data_daily", total_rows)
    except Exception as e:
        logger.info(e, exc_info=True)

        # send_email(
        #     email_config["receiver"],
        #     e,
        # )
