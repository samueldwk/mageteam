# Criar view do relatorio geral de ecommerce da Lofty

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os
import gspread
from google.oauth2.service_account import Credentials

# import ads_meta_lofty as fb

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
dx = date.today() - timedelta(days=3)  # x DATE
# d1 = datetime(2025, 2, 15).date()  # SELECT DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()

# CLIENT LIST

c_list = ["lofty"]

# DICIONÃRIO DE NOMES

dic_nomes = {
    "lofty": "Lofty",
}

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_LOFTY_URL")
key: str = os.environ.get("SUPABASE_LOFTY_KEY")
supabase: Client = create_client(url, key)

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

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)

for client in c_list:
    # In[4]: Bater no db e trazer stock summary

    def get_metaAds(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("MetaAds")
                    .select("*")
                    .order(
                        "ads_meta_date", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("ads_meta_date", dataname1)
                    .lte("ads_meta_date", dataname1)
                    .execute()
                )

                if response.data:
                    # print(
                    #     f"Retrieved {len(response.data)} rows"
                    # )  # Debugging
                    all_data.extend(response.data)
                    if len(response.data) < limit - 1:
                        break  # Stop when fewer than 'limit' records are returned
                    offset += len(
                        response.data
                    )  # Move offset by actual rows received
                else:
                    # print("â ï¸ No data returned, stopping pagination.")
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(f"Error fetching metaads data for client {client}: {e}")
            raise

    try:
        rows_stock = get_metaAds(client)
        if rows_stock:
            df_metaads = pd.DataFrame(rows_stock)
        else:
            print(f"No metaAds data found for client: {client}")

    except Exception as e:
        print(e)

    # Organizar df para view final

    # Select and reorder the required rows
    df_metaads = df_metaads.set_index("ads_meta_date")

    df_metaads_final = df_metaads.loc[
        :, ["ads_meta_investment", "ads_meta_sale", "ads_meta_roas"]
    ].T

    # In[7]: Save df_summaryStock_value_final in google sheets

    gc = gc = gspread.authorize(credentials)

    sh = gc.open(
        f"{dic_nomes[client]} - Relatório Geral E-Commerce"
    ).worksheet("Geral")

    # Get all values from Google Sheets
    sheet_data = sh.get_all_values()
    df_sheet = pd.DataFrame(sheet_data)

    # Step 4: Get date columns from Google Sheets (assuming first row contains date headers)
    date_columns = [
        str(date).strip() for date in df_sheet.iloc[1].tolist()
    ]  # Normalize date format

    # Step 5: Ensure row labels are accessible
    df_metaads_final = df_metaads_final.reset_index()

    # Step 6: Define row mappings (info types)
    info_types = {
        "Investimento FaceAds": "ads_meta_investment",
        "Venda FaceAds": "ads_meta_sale",
        "ROI FaceAds": "ads_meta_roas",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_metaads_final.columns[
        1:
    ]:  # Skip first column (row labels)
        order_date_str = pd.to_datetime(order_date, errors="coerce").strftime(
            "%Y-%m-%d"
        )  # Normalize date format

        if order_date_str not in date_columns:
            print(f"⚠️ Date {order_date_str} not found in Google Sheets!")
            continue  # Skip if date is not found

        col_idx = (
            date_columns.index(order_date_str) + 1
        )  # Google Sheets uses 1-based indexing

        # Step 10: Loop through DataFrame row labels and update values
        for i, df_label in enumerate(df_metaads_final.iloc[:, 0]):
            # ✅ Convert DataFrame row label to Google Sheets row label using `info_types`
            gs_label = next(
                (
                    key
                    for key, value in info_types.items()
                    if value == df_label
                ),
                None,
            )

            if not gs_label:
                print(
                    f"⚠️ No matching Google Sheets row found for DataFrame row: '{df_label}'"
                )
                continue  # Skip if no match found

            # ✅ Find row index in Google Sheets
            gs_label_lower = gs_label.lower()

            if gs_label_lower in row_labels:
                row_idx = (
                    row_labels.index(gs_label_lower) + 1
                )  # Convert to 1-based index
            else:
                print(f"⚠️ Row for '{gs_label}' not found in Google Sheets!")
                continue  # Skip if row label is not found

            # ✅ Get value from DataFrame
            value = df_metaads_final.iloc[
                i,
                df_metaads_final.columns.get_loc(order_date),
            ]

            # ✅ Add batch update
            updates.append(
                {
                    "range": f"{gspread.utils.rowcol_to_a1(row_idx, col_idx)}",
                    "values": [[value]],
                }
            )

    # Step 11: Execute batch update in Google Sheets
    if updates:
        sh.batch_update(updates)
        print(
            f"✅ Successfully updated [df_metaads_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )
