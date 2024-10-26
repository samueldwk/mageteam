# Pegar dados do banco e atualizar google sheets

import dotenv
from pushover_notification import send_pushover_notification
import os
from supabase import create_client, Client
import date_functions_retroativo as datef
from datetime import datetime
import pandas as pd
import gspread


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime(2024, 10, 11).date()

(
    datatxt,
    dataname1,
    datasql,
    datatxt2,
    dataname2,
    datatxt3,
    dataname3,
) = datef.dates(d1)

dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
datanam2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()

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

c_list = ["french", "talgui"]

# DICIONÁRIO DE NOMES
dic_nomes = {
    "alanis": "Alanis",
    "basicler": "Basicler",
    "dadri": "Dadri",
    "french": "French",
    "haut": "Haut",
    "infini": "Infini",
    "kle": "Kle",
    "luvic": "Luvic",
    "morina": "Morina",
    "mun": "Mun",
    "muna": "Muna",
    "nobu": "Nobu",
    "othergirls": "Other Girls",
    "paconcept": "P.A Concept",
    "rery": "Rery",
    "talgui": "Talgui",
    "una": "Una",
    "uniquechic": "Unique Chic",
}


# Initialize Supabase client
SUPABASE_URL = os.environ.get("SUPABASE_BI_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_BI_KEY")


def get_row_by_two_columns(
    client, dataname3_date_format, dataname_date_format
):
    try:
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
    except Exception as e:
        # Print the error to debug more effectively
        print(f"Error fetching data for client {client}: {e}")
        raise


# Call the function

for client in c_list:
    # Run in git actions
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "mage---performan-1705337009329-52b7dddd6d54.json"

    try:
        rows = get_row_by_two_columns(
            client, dataname3_date_format, dataname1_date_format
        )
        if rows:
            for row in rows:
                print("Matching row found:", row)
            df_relatorio_gerencial = pd.DataFrame(rows)
        else:
            print(f"No matching row found for client: {client}")

        # Transformar dados para ficarem igual ao view

        # Data (transformar formato e colocar colunas mês e ano)
        df_relatorio_gerencial["data"] = pd.to_datetime(
            df_relatorio_gerencial["data"], format="%Y-%m-%d"
        )

        df_relatorio_gerencial["mes"] = df_relatorio_gerencial["data"].dt.month
        df_relatorio_gerencial["ano"] = df_relatorio_gerencial["data"].dt.year

        df_relatorio_gerencial["data"] = pd.to_datetime(
            df_relatorio_gerencial["data"]
        ).dt.strftime("%d/%m/%Y")

        # Columns to keep da df final feito para ir pra view do cliente
        columns_to_keep = [
            "data",
            "mes",
            "ano",
            "vendas_valor",
            "vendas_mkp",
            "vendas_ticket_medio",
            "vendas_preco_medio",
            "estoque_quantidade",
            "estoque_valor_venda",
            "estoque_mkp",
            "estoque_preco_medio",
            "vendas_desconto_0",
            "vendas_desconto_media_15",
            "vendas_desconto_media_30",
            "vendas_desconto_media_50",
            "vendas_desconto_media_70",
            "estoque_desconto_0",
            "estoque_desconto_media_15",
            "estoque_desconto_media_30",
            "estoque_desconto_media_50",
            "estoque_desconto_media_70",
            "fb_investido",
            "fb_valor_venda",
            "fb_roas",
            "fb_cpm",
            "fb_cpc",
            "fb_ctr",
            "ga_sessions",
            "ga_bouncerate",
            "ga_conversionrate",
        ]

        df_relatorio_gerencial = df_relatorio_gerencial[columns_to_keep]

        # %% UPDATE GOOGLE SHEETS

        # # Run in local computer
        # gc = gspread.oauth()

        # Run in gitactions
        try:
            gc = gspread.service_account(
                filename="mage---performan-1705337009329-52b7dddd6d54.json"
            )
            print("Service account successfully authenticated")
        except Exception as e:
            print(f"Error authenticating with service account: {e}")

        sh = gc.open(
            f"{dic_nomes[client]} - Relatório Gerencial E-Commerce"
        ).worksheet("Cópia de Diário")

        def update_row_by_date(list_of_rows):
            for row_data in list_of_rows:
                date_value = row_data[0]  # First element is the date

                try:
                    # Find the row number for the matching date
                    cell = sh.find(date_value)
                    row_number = cell.row

                    # Define the range to update (e.g., A to V columns)
                    range_name = f"A{row_number}:AD{row_number}"

                    # Update the row with the current row_data
                    sh.update(range_name, [row_data])
                    print(f"Updated row {row_number} with data: {row_data}")
                except gspread.exceptions.CellNotFound:
                    print(f"Date {date_value} not found in the sheet.")

        list_relatorio_gerencial = df_relatorio_gerencial.values.tolist()
        # print(list_relatorio_gerencial)

        update_row_by_date(list_relatorio_gerencial)

    except Exception as e:
        print(
            f"*****ERRO: {client} | atualizar_relatorio_gerencial | {e} [{dataname1}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | atualizar_relatorio_gerencial | {e} [{dataname1}]"
        )
