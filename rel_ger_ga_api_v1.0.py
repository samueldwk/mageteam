# rel_ger_ga_api_v1.0
# REL GER GA BY API

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time
import numpy as np

dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt1, dataname1, datasql, dataname2 = datef.dates(d1)

date_object = datetime.datetime.strptime(dataname1, "%Y-%m-%d")

month = date_object.month
year = date_object.year

# CLIENT LIST
c_list = [
    # "ajobrand",
    "alanis",
    "dadri",
    "french",
    # "haverroth",
    "haut",
    "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
]

# c_list = ["alanis", "mun"]


for client in c_list:
    # In[01]: Google Analytics API e montar df final de google analytics

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

    # Run in local machine
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

    # Using a default constructor instructs the client to use the credentials
    # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
    client_ga = BetaAnalyticsDataClient()

    # REQUEST DATA TO GA API - run request method

    request = RunReportRequest(
        property=f"properties/{os.getenv(f'ga_id_{client}')}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="bounceRate"),
            Metric(name="engagedSessions"),
            Metric(name="addToCarts"),
            Metric(name="checkouts"),
            Metric(name="sessionConversionRate"),
            Metric(name="ecommercePurchases"),
        ],
        date_ranges=[DateRange(start_date=dataname2, end_date=dataname1)],
    )

    # FORMAT REPORT - run report method

    response_ga = client_ga.run_report(request)
    # print(response_ga)

    # Row index
    row_index_names = [header.name for header in response_ga.dimension_headers]
    row_header = []
    for i in range(len(row_index_names)):
        row_header.append(
            [row.dimension_values[i].value for row in response_ga.rows]
        )

    row_index_named = pd.MultiIndex.from_arrays(
        np.array(row_header), names=np.array(row_index_names)
    )
    # Row flat data
    metric_names = [header.name for header in response_ga.metric_headers]
    data_values = []
    for i in range(len(metric_names)):
        data_values.append(
            [row.metric_values[i].value for row in response_ga.rows]
        )

    df_relger_ga_final = pd.DataFrame(
        data=np.transpose(np.array(data_values, dtype="f")),
        index=row_index_named,
        columns=metric_names,
    ).reset_index()

    df_relger_ga_final["DATA"] = df_relger_ga_final["date"].apply(
        lambda x: datetime.datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")
    )

    df_relger_ga_final["DATA API"] = dataname1

    df_relger_ga_final.drop(columns="date", inplace=True)

    # In[02]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    # email: str = os.environ.get("supabase_email")
    # password: str = os.environ.get("supabase_password")

    # data = Client.sign_in(
    #     {"email": email, "password": password}
    # )

    dic_relger_ga_final = df_relger_ga_final.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"df_relger_ga_{client}")
            .upsert(dic_relger_ga_final)
            .execute()
        )

    except Exception as exception:
        print(exception)
