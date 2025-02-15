import os
import pandas as pd
import numpy as np
import dotenv
import date_functions as datef
from datetime import timedelta, date

dotenv.load_dotenv()

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "C:\\Users\\Samuel Kim\\OneDrive\\Documentos\\bat\\credentials.json"

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

# Using a default constructor instructs the client to use the credentials
# specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
client = BetaAnalyticsDataClient()

### DATA FUNCTIONS

d2 = date.today() - timedelta(days=2)
datatxt2, dataname2, datasql2, datanamex, dataname3, dataname4 = datef.dates(
    d2
)

d1 = date.today() - timedelta(days=1)
datatxt1, dataname1, datasql1, datanamex, dataname3, dataname4 = datef.dates(
    d1
)

# CLIENTES

c_list = ["lofty"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_LOFTY_URL")
key: str = os.environ.get("SUPABASE_LOFTY_KEY")
supabase: Client = create_client(url, key)


# %% FORMAT REPORT - run report method


def format_report(request):
    response = client.run_report(request)

    # Row index
    row_index_names = [header.name for header in response.dimension_headers]
    row_header = []
    for i in range(len(row_index_names)):
        row_header.append(
            [row.dimension_values[i].value for row in response.rows]
        )

    row_index_named = pd.MultiIndex.from_arrays(
        np.array(row_header), names=np.array(row_index_names)
    )
    # Row flat data
    metric_names = [header.name for header in response.metric_headers]
    data_values = []
    for i in range(len(metric_names)):
        data_values.append(
            [row.metric_values[i].value for row in response.rows]
        )

    output = pd.DataFrame(
        data=np.transpose(np.array(data_values, dtype="f")),
        index=row_index_named,
        columns=metric_names,
    )
    return output


# %% REQUEST DATA TO GA API - run request method


def download_ga(cliente, dataname2, dataname1):
    request = RunReportRequest(
        property=f"properties/{os.getenv(f'ga_id_{cliente}')}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="sessionKeyEventRate"),
            Metric(name="bounceRate"),
        ],
        date_ranges=[DateRange(start_date=dataname2, end_date=dataname1)],
    )

    # Generate and format the report
    output_df = format_report(request)

    return output_df[["sessions", "bounceRate", "sessionKeyEventRate"]]


for cliente in c_list:
    df_rel_ger_ga = download_ga(cliente, dataname2, dataname1)

    # Adicionar a coluna de data do g.a

    # Convert index to a column
    df_rel_ger_ga.reset_index(inplace=True)

    # Rename the index column to 'date'
    df_rel_ger_ga.rename(columns={"index": "date"}, inplace=True)

    # Convert 'date' to string and then to datetime format
    df_rel_ger_ga["ga_date"] = pd.to_datetime(
        df_rel_ger_ga["date"].astype(str), format="%Y%m%d"
    ).dt.strftime("%Y-%m-%d")

    # Drop the old 'date' column if no longer needed
    df_rel_ger_ga.drop(columns=["date"], inplace=True)

    # In[6]: Gravar informações de ga na db

    dic_rel_ger_ga = df_rel_ger_ga.to_dict(orient="records")

    try:
        response = (
            supabase.table("GoogleAnalytics")
            .upsert(dic_rel_ger_ga, returning="minimal")
            .execute()
        )

    except Exception as e:
        print(e)

    except Exception as exception:
        print(f"*****ERRO: UPSERT {client}: {exception}")
