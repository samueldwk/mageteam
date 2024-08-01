# ga_funil_v1

import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import numpy as np
import gspread
from datetime import timedelta, date


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

### DATA FUNCTIONS

d2 = date.today() - timedelta(days=6)
datatxt2, dataname2, datasql2, datanamex = datef.dates(d2)

d1 = date.today() - timedelta(days=5)
datatxt1, dataname1, datasql1, datanamex = datef.dates(d1)

# CLIENT LIST
c_list = [
    "alanis",
    "dadri",
    "french",
    "haut",
    "infini",
    "kle",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
]

c_list = ["talgui"]

# DICIONÁRIO DE NOMES

dic_nomes = {
    "ajobrand": "aJo Brand",
    "alanis": "Alanis",
    "dadri": "Dadri",
    "french": "French",
    "haverroth": "Haverroth",
    "haut": "Haut",
    "infini": "Infini",
    "kle": "Kle",
    "luvic": "Luvic",
    "mun": "Mun",
    "nobu": "Nobu",
    "othergirls": "Other Girls",
    "paconcept": "P.A Concept",
    "rery": "Rery",
    "talgui": "Talgui",
    "una": "Una",
    "uniquechic": "Unique Chic",
}

for client in c_list:
    # In[01]: Google Analytics API e montar df final de google analytics

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

    # Run in local machine
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
    client_ga = BetaAnalyticsDataClient()

    # REQUEST DATA TO GA API - run request method

    request = RunReportRequest(
        property=f"properties/{os.getenv(f'ga_id_{client}')}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            # Metric(name="bounceRate"),
            # Metric(name="totalUsers"),
            Metric(name="engagedSessions"),
            Metric(name="activeUsers"),
            Metric(name="itemViewEvents"),
            Metric(name="addToCarts"),
            Metric(name="checkouts"),
            Metric(name="ecommercePurchases"),
            # Metric(name="keyEvents"),
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

    df_funil_ga_final = pd.DataFrame(
        data=np.transpose(np.array(data_values, dtype="f")),
        index=row_index_named,
        columns=metric_names,
    ).reset_index()

    df_funil_ga_final["DATA"] = df_funil_ga_final["date"].apply(
        lambda x: datetime.datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")
    )

    df_funil_ga_final["DATA API"] = dataname1

    df_funil_ga_final.drop(columns="date", inplace=True)

    # In[02]: Enviar informações para DB

    # from supabase import create_client, Client
    # import supabase

    # url: str = os.environ.get("SUPABASE_URL")
    # key: str = os.environ.get("SUPABASE_KEY")
    # supabase: Client = create_client(url, key)

    # # email: str = os.environ.get("supabase_email")
    # # password: str = os.environ.get("supabase_password")

    # # data = Client.sign_in(
    # #     {"email": email, "password": password}
    # # )

    # dic_relger_ga_final = df_relger_ga_final.to_dict(orient="records")

    # try:
    #     response = (
    #         supabase.table(f"df_relger_ga_{client}")
    #         .upsert(dic_relger_ga_final)
    #         .execute()
    #     )

    # except Exception as exception:
    #     print(exception)

    # %% UPDATE GOOGLE SHEETS

    gc = gspread.oauth()

    sh = gc.open(f"{dic_nomes[client]} - Relatório Funil").worksheet("Diário")

    list_funil_final = df_funil_ga_final.values.tolist()

    print(list_funil_final)

    sh.append_rows(list_funil_final, table_range="A1")

    gc = gspread.oauth()

    list_funil_final = df_funil_ga_final.values.tolist()

    # sh = gc.open(f"{dic_nomes[client]} - Relatório Funil").worksheet("Diário")

    # cell_d2 = sh.find(datatxt2).row

    # sh.update_cell(cell_d2, 29, list_final[0][0])

    # sh.update_cell(cell_d2, 30, list_final[0][1])

    # cell_d1 = sh.find(datatxt1).row

    # sh.update_cell(cell_d1, 29, list_final[1][0])

    # sh.update_cell(cell_d1, 30, list_final[1][1])
