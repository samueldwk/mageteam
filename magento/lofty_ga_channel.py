import os
import pandas as pd
import numpy as np
import dotenv
import date_functions as datef
from datetime import timedelta, date
from google.oauth2.service_account import Credentials

dotenv.load_dotenv()

# os.environ[
#     "GOOGLE_APPLICATION_CREDENTIALS"
# ] = "C:\\Users\\Samuel Kim\\OneDrive\\Documentos\\bat\\credentials.json"

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

service_account_info = {
    "type": os.environ.get("SERVICE_ACCOUNT_TYPE"),
    "project_id": os.environ.get("SERVICE_ACCOUNT_PROJECT_ID"),
    "private_key_id": os.environ.get("SERVICE_ACCOUNT_PRIVATE_KEY_ID"),
    "private_key": os.environ.get("SERVICE_ACCOUNT_PRIVATE_KEY").replace("\\n", "\n"),
    "client_email": os.environ.get("SERVICE_ACCOUNT_CLIENT_EMAIL"),
    "client_id": os.environ.get("SERVICE_ACCOUNT_CLIENT_ID"),
    "auth_uri": os.environ.get("SERVICE_ACCOUNT_AUTH_URI"),
    "token_uri": os.environ.get("SERVICE_ACCOUNT_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.environ.get("SERVICE_ACCOUNT_AUTH_PROVIDER"),
    "client_x509_cert_url": os.environ.get("SERVICE_ACCOUNT_CERT_URL"),
    "universe_domain": os.environ.get("SERVICE_ACCOUNT_UNIVERSE_DOMAIN", "googleapis.com")
}

# Create credentials object with appropriate scopes for Google Analytics
scopes = ["https://www.googleapis.com/auth/analytics", 
          "https://www.googleapis.com/auth/analytics.readonly"]

credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)

# Using a default constructor instructs the client to use the credentials
# specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
client = BetaAnalyticsDataClient(credentials=credentials)

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_LOFTY_URL")
key: str = os.environ.get("SUPABASE_LOFTY_KEY")
supabase: Client = create_client(url, key)

# DATA FUNCTIONS

d1 = date.today() - timedelta(days=1)
datatxt1, dataname1, datasql1, dataname2, dataname3, dataname4 = datef.dates(
    d1
)

# dataname2 = "2025-02-25"


def get_ga_df():
    request = RunReportRequest(
        property=f"properties/{os.getenv('ga_id_lofty')}",
        date_ranges=[{"start_date": dataname2, "end_date": dataname1}],
        dimensions=[
            {"name": "date"},  # New: Adds Date as a dimension
            {"name": "sessionSource"},  # Traffic source
            {"name": "sessionMedium"},  # Traffic medium
            {"name": "sessionCampaignName"},  # Campaign name
            {"name": "sessionPrimaryChannelGroup"},  # Channel Group
        ],
        metrics=[
            {"name": "totalRevenue"},  # Total sales revenue
            # {"name": "sessions"},  # Total sessions
            # {"name": "engagedSessions"},  # Engaged sessions
            # {"name": "conversions"},  # Converted sessions
            # {"name": "bounceRate"},  # Bounce rate
            # {"name": "sessionConversionRate"},  # Conversion rate
            # {"name": "averageSessionDuration"},  # Avg. session duration
            # {"name": "screenPageViews"},  # Total page views
        ],
    )

    response = client.run_report(request)

    # Convert response to a DataFrame
    data = []
    for row in response.rows:
        date = row.dimension_values[0].value  # New: Extract Date
        source = row.dimension_values[1].value
        medium = row.dimension_values[2].value
        campaign = row.dimension_values[3].value
        channel_group = row.dimension_values[4].value  # Channel Group

        data.append(
            {
                "ga_date": date,  # New: Include Date
                "Source": source,
                "Medium": medium,
                "Campaign Name": campaign,
                "ga_channelgroup": channel_group,
                "ga_order_value": float(row.metric_values[0].value),
                # "ga_sessions": int(row.metric_values[1].value),
                # "ga_engaged_sessions": int(row.metric_values[2].value),
                # "ga_converted_sessions": int(row.metric_values[3].value),
                # "Bounce Rate (%)": float(row.metric_values[4].value) * 100,
                # "Session Conversion Rate (%)": float(
                #     row.metric_values[5].value
                # )
                # * 100,
                # "ga_timespent": float(row.metric_values[6].value),
                # "ga_pageviews": int(row.metric_values[7].value),
            }
        )

    df = pd.DataFrame(data)

    return df


df_ga = get_ga_df()

# Tratar dados errados (Channel Group)

# Ensure source and medium are lowercase for comparison
df_ga["Source"] = df_ga["Source"].str.lower()
df_ga["Medium"] = df_ga["Medium"].str.lower()

# Apply conditions to modify the 'Channel Group' column
df_ga["ga_channelgroup"] = np.where(
    ((df_ga["Source"] == "facebook") & (df_ga["Medium"] == "ads"))
    | (df_ga["Source"] == "facebook_ads"),
    "Paid Traffic",
    df_ga["ga_channelgroup"],  # Keep existing value if condition is not met
)

# Groupby by value on Channel Group

df_ga_channelgroup = df_ga.groupby(
    ["ga_date", "ga_channelgroup"], as_index=False
).agg(
    {"ga_order_value": "sum"}  # Sum of order values per date & channel group
)


# Gravar informações de df_ga_channelgroup na db

dic_ga_channelgroup = df_ga_channelgroup.to_dict(orient="records")

try:
    response = (
        supabase.table("GoogleAnalyticsChannel")
        .upsert(dic_ga_channelgroup, returning="minimal")
        .execute()
    )

except Exception as e:
    print(e)
