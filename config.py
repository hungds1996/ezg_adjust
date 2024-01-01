import datetime
from google.cloud import bigquery


end_date = str(datetime.date.today() - datetime.timedelta(days=1))
json_keys = 'data-import-409408-97adc8924579.json'

api_token = 'yJLV5vBb2-LhGpc4Tukj'
apps_token = ['9qr22ik5f30g','weh64bmgqg3k']
kpis = ['install_cost', 'click_cost', 'impression_cost','cost','ad_impressions','ad_revenue','paid_installs','paid_clicks','paid_impressions','clicks', 'impressions', 'installs']
groupby='apps,days,networks,countries'

temp_file = ".\\data\\adjuts_data.csv "

app_info = [
    {
        "app_name": "Hero Survival",
        "info": {
                    "table_id": "data-import-409408.temp_tables.hero_survival_adjust",
                    "app_token": "9qr22ik5f30g",
                    "app_id": "hero_survival"
                }
    },
    {
        "app_name": "Nightfall",
        "info": {
                    "table_id": "data-import-409408.temp_tables.nightfall_adjust",
                    "app_token": "weh64bmgqg3k",
                    "app_id": "nightfall"
                }
    }
]

email_config = {
    "sender": "hungds1996@gmail.com",
    "password": "pvbm vfld coeq wohw",
    "receiver": ["hungds1996@gmail.com"],
}