import os
import pandas as pd
import numpy as np
import dotenv
import date_functions as datef
from datetime import timedelta, date
import gspread

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
datatxt2, dataname2, datasql2, datanamex = datef.dates(d2)

d1 = date.today() - timedelta(days=1)
datatxt1, dataname1, datasql1, datanamex = datef.dates(d1)

# CLIENTES

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
    # "rery",
    "talgui",
    "una",
    "uniquechic",
]

# c_list = ["uniquechic"]

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
            Metric(name="sessionKeyEventRate"),
            Metric(name="bounceRate"),
        ],
        date_ranges=[DateRange(start_date=dataname2, end_date=dataname1)],
    )

    output_df = format_report(request)

    return output_df[["bounceRate", "sessionKeyEventRate"]]


for cliente in c_list:
    df_rel_ger_ga = download_ga(cliente, dataname2, dataname1)
    df_rel_ger_ga = df_rel_ger_ga.sort_index(ascending=True)

    # %% UPDATE GOOGLE SHEETS

    gc = gspread.oauth()

    list_final = df_rel_ger_ga.values.tolist()

    sh = gc.open(
        f"{dic_nomes[cliente]} - Relatório Gerencial E-Commerce"
    ).worksheet("Diário")

    cell_d2 = sh.find(datatxt2).row

    sh.update_cell(cell_d2, 29, list_final[0][0])

    sh.update_cell(cell_d2, 30, list_final[0][1])

    cell_d1 = sh.find(datatxt1).row

    sh.update_cell(cell_d1, 29, list_final[1][0])

    sh.update_cell(cell_d1, 30, list_final[1][1])
