# Pegar dados do banco e atualizar google sheets

import dotenv
from pushover_notification import send_pushover_notification
import os
from supabase import create_client, Client
import date_functions as datef
from datetime import datetime
import pandas as pd


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
datanam2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
dataname_date_format = datetime.strptime(dataname, "%Y-%m-%d").date()

# CLIENT LIST
c_list = [
    "alanis",
    "basicler",
    # "dadri",
    "french",
    "haut",
    # "infini",
    "kle",
    "mun",
    # "muna",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
    "uniquechic",
]

c_list = ["french"]

# Initialize Supabase client
SUPABASE_URL = os.environ.get("SUPABASE_BI_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_BI_KEY")


def get_row_by_two_columns():
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    response = (
        supabase.table("relatorio_gerencial_geral")
        .select("*")
        # .eq("data", f"{dataname_date_format}")
        .gte("data", dataname3_date_format)
        .lte("data", dataname_date_format)
        .eq("mage_cliente", f"{client}")
        .execute()
    )

    # Check if a matching row exists
    if response.data:
        return response.data  # Returns all matching rows
    else:
        return None


# Call the function

for client in c_list:
    try:
        rows = get_row_by_two_columns()
        if rows:
            for row in rows:
                print("Matching row found:", row)
        else:
            print("No matching row found")

        df_relatorio_gerencial = pd.DataFrame(rows)

        # columns_to_keep = [
        #     ]

    except Exception as e:
        print(
            f"*****ERRO: {client} | atualizar_relatorio_gerencial | {e} [{dataname}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | atualizar_relatorio_gerencial | {e} [{dataname}]"
        )
