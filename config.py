import datetime
from google.cloud import bigquery

# default end date is yesterday
end_date = str(datetime.date.today() - datetime.timedelta(days=1))

# file name of service account json key
json_keys = "data-import-409408-97adc8924579.json"

# adjust account api token
# api_token = "<adjust API token>"
api_token = "yJLV5vBb2-LhGpc4Tukj"

# app token in adjust dashboard
apps_token = ["9qr22ik5f30g", "weh64bmgqg3k"]

# Metrics and dimensions
kpis = "installs,network_installs,clicks,network_clicks,impressions,network_impressions,organic_installs,cost,adjust_cost,network_cost,click_cost,install_cost,impression_cost,ad_revenue,attribution_clicks"
groupby = "app_token,day,app,partner_name,campaign,network,country,country_code,adgroup_network,adgroup,store_id,store_type,source_network"

# local temp file name
temp_file = ".\\data\\adjuts_data.csv "

# game info include app name, table id (bigquery table) , app token, app id
app_info = [
    {
        "app_name": "Hero Survival",
        "info": {
            "table_id": "data-import-409408.temp_tables.hero_survival_adjust",
            "app_token": "9qr22ik5f30g",
            "app_id": "hero_survival",
        },
    },
    {
        "app_name": "Nightfall",
        "info": {
            "table_id": "data-import-409408.temp_tables.nightfall_adjust",
            "app_token": "weh64bmgqg3k",
            "app_id": "nightfall",
        },
    },
]

# email account to send mail
email_config = {
    "sender": "<email address>",
    "password": "<email password or app password>",
    "receiver": ["hungds1996@gmail.com"],
}
