# Criar view do relatorio geral de ecommerce da Lofty

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os
import gspread

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()

# dataname3 = "2025-02-25"

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


for client in c_list:
    # In[4]: Bater no db e trazer googleanalyticssummary

    def get_googleAnalyticsSummary(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("GoogleAnalyticsSummary")
                    .select("*")
                    .order("ga_date", desc=False)  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("ga_date", dataname3)
                    .lte("ga_date", dataname1)
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
            print(
                f"Error fetching GoogleAnalyticsSummary data for client {client}: {e}"
            )
            raise

    try:
        rows_stock = get_googleAnalyticsSummary(client)
        if rows_stock:
            df_googleAnalyticsSummary = pd.DataFrame(rows_stock)
        else:
            print(f"No GoogleAnalyticsSummary data found for client: {client}")

    except Exception as e:
        print(e)

    # Organizar df para view final

    # Select and reorder the required rows
    df_googleAnalyticsSummary = df_googleAnalyticsSummary.set_index("ga_date")

    df_googleAnalyticsSummary_final = df_googleAnalyticsSummary.loc[
        :,
        [
            "ga_sessions",
            "ga_engaged_sessions",
            "ga_converted_sessions",
            "ga_bounce_rate",
            "ga_conversion_rate",
            "ga_pageviews",
            "ga_order_value",
            "ga_timespent",
            "ga_timespent_total",
        ],
    ].T

    # In[7]: Save df_googleads_institucional_final in google sheets

    gc = gspread.oauth()

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
    df_googleAnalyticsSummary_final = (
        df_googleAnalyticsSummary_final.reset_index()
    )

    # Step 6: Define row mappings (info types)
    info_types = {
        "Venda Google Analytics (R$)": "ga_order_value",
        "Taxa de Conversão": "ga_conversion_rate",
        "Taxa de Rejeição": "ga_bounce_rate",
        "Sessões": "ga_sessions",
        "Sessões (Engajadas)": "ga_engaged_sessions",
        "Sessões (Convertidas)": "ga_converted_sessions",
        "Visualizações de Páginas": "ga_pageviews",
        "Tempo Médio na Página (segundos)": "ga_timespent",
        "Tempo Total na Página (segundos)": "ga_timespent_total",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_googleAnalyticsSummary_final.columns[
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
        for i, df_label in enumerate(
            df_googleAnalyticsSummary_final.iloc[:, 0]
        ):
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
            value = df_googleAnalyticsSummary_final.iloc[
                i,
                df_googleAnalyticsSummary_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_googleAnalyticsSummary_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[4]: Bater no db e trazer googleanalytics channel

    def get_googleAnalyticsChannel(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("GoogleAnalyticsChannel")
                    .select("*")
                    .order("ga_date", desc=False)  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("ga_date", dataname3)
                    .lte("ga_date", dataname1)
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
            print(
                f"Error fetching GoogleAnalyticsChannel data for client {client}: {e}"
            )
            raise

    try:
        rows_stock = get_googleAnalyticsChannel(client)
        if rows_stock:
            df_googleAnalyticsChannel = pd.DataFrame(rows_stock)
        else:
            print(f"No GoogleAnalyticsChannel data found for client: {client}")

    except Exception as e:
        print(e)

    # Organizar df para view final

    # Select and reorder the required rows

    df_googleAnalyticsChannel["date_channel"] = (
        df_googleAnalyticsChannel["ga_date"]
        + df_googleAnalyticsChannel["ga_channelgroup"]
    )

    df_googleAnalyticsChannel = df_googleAnalyticsChannel.set_index("ga_date")

    df_googleAnalyticsChannel_final = df_googleAnalyticsChannel.pivot_table(
        index="ga_channelgroup",  # Keep this as rows
        columns="ga_date",  # Move this to columns
        values="ga_order_value",  # Values to fill the table
        aggfunc="sum",  # Ensure aggregation if needed
    ).reset_index()  # Keep 'product_discount_range' as a column

    df_googleAnalyticsChannel_final.fillna(0, inplace=True)

    df_googleAnalyticsChannel_final.rename(
        columns={"date_channel": "index"}, inplace=True
    )

    # In[7]: Save df_googleAnalyticsChannel_final in google sheets

    gc = gspread.oauth()

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

    # Step 6: Define row mappings (info types)
    info_types = {
        "SMS": "SMS",
        "Whatsapp": "Whatsapp",
        "E-mail Mkt": "Email",
        "Social": "Organic Social",
        "Referral": "Referral",
        "Direct": "Direct",
        "Busca Orgânica": "Organic Search",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_googleAnalyticsChannel_final.columns[
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
        for i, df_label in enumerate(
            df_googleAnalyticsChannel_final.iloc[:, 0]
        ):
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
            value = df_googleAnalyticsChannel_final.iloc[
                i,
                df_googleAnalyticsChannel_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_googleAnalyticsChannel_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )
