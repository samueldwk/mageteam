# Criar view do relatorio geral de ecommerce da Lofty

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os
import math
import concurrent.futures
import gspread
import numpy as np

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
dx = date.today() - timedelta(days=15)  # x DATE
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

# Convert to GMT 0 by adding 3 hours

start_date = (
    datetime.combine(dx, datetime.min.time()) + timedelta(hours=3)
).isoformat()
end_date = (
    datetime.combine(d1, datetime.min.time())
    + timedelta(hours=3, seconds=86399)
).isoformat()

for client in c_list:
    # In[1]: Bater no db e pegar lista de pedidos

    def get_order(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("Order")
                    .select("*")
                    .order(
                        "order_id", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("order_create_date", start_date)
                    .lte("order_create_date", end_date)
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
            print(f"Error fetching order data for client {client}: {e}")
            raise

    rows_order = get_order(client)
    if rows_order:
        df_order = pd.DataFrame(rows_order)
    else:
        print(f"No order data found for client: {client}")

    # In[2]: Tratar dados para criar view vendas magento por status

    # Criar coluna order_create_date_brazil tranforando UTM 0 em UTM -3 e tirando horário
    df_order["order_create_date"] = pd.to_datetime(
        df_order["order_create_date"], errors="coerce"
    )

    df_order.loc[:, "order_create_date_brazil"] = (
        df_order["order_create_date"]
        .dt.tz_localize("UTC")  # Set original timezone as UTC
        .dt.tz_convert("America/Sao_Paulo")  # Convert to Brazil time (UTC-3)
        .dt.date  # Extract only the date (YYYY-MM-DD)
    )

    # Somar total do valor das vendas magento, por status, separando por dia
    df_view_vendaMagento_valor_status = (
        df_order.groupby(["order_create_date_brazil", "order_status"])[
            "order_total_value"
        ]
        .sum()
        .unstack(fill_value=0)  # Converts status into columns
        .reset_index()
    )

    df_view_vendaMagento_valor_status[
        "paid"
    ] = df_view_vendaMagento_valor_status.get(
        "complete", 0
    ) + df_view_vendaMagento_valor_status.get(
        "processing", 0
    )  # venda de status processing e complete

    df_view_vendaMagento_valor_status[
        "total"
    ] = df_view_vendaMagento_valor_status[
        ["canceled", "processing", "complete"]
    ].sum(
        axis=1
    )

    df_view_vendaMagento_valor_status["canceled_%"] = (
        df_view_vendaMagento_valor_status["canceled"]
        / df_view_vendaMagento_valor_status["total"]
    )

    df_view_vendaMagento_valor_status["complete_%"] = (
        df_view_vendaMagento_valor_status["complete"]
        / df_view_vendaMagento_valor_status["paid"]
    )

    # Organizar df para view final

    # Select and reorder the required rows
    df_view_vendaMagento_valor_status = (
        df_view_vendaMagento_valor_status.set_index("order_create_date_brazil")
    )

    df_view_vendaMagento_valor_status_final = df_view_vendaMagento_valor_status.loc[
        :,
        [
            "total",
            "canceled",
            "canceled_%",
            "paid",
            "complete_%",
            "complete",
            # "processing",
        ],
    ].T

    # In[7]: Save df_view_vendaMagento_valor_status_final in google sheets

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
    df_view_vendaMagento_valor_status_final = (
        df_view_vendaMagento_valor_status_final.reset_index()
    )

    # Step 6: Define row mappings (info types)
    info_types = {
        "Venda Magento (Geral)(todos status pedidos)(R$)": "total",
        "Venda Magento (Geral)(PIX não pago)(R$)": "canceled",
        "Venda Magento (Geral)(PIX não pago)(% sob total)": "canceled_%",
        "Venda Magento (Geral)(apenas pagos)(R$)": "paid",
        "Venda Magento (Geral)(faturado)(% sob total pago)": "complete_%",
        "Venda Magento (Geral)(faturado)(R$)": "complete",
        # "Venda Magento (Geral)(pago não faturado)(R$)": "processing",
        # "Venda Magento (Geral)(pago não faturado)(qtd pedidos)": "processing_%",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_view_vendaMagento_valor_status_final.columns[
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
            df_view_vendaMagento_valor_status_final.iloc[:, 0]
        ):
            df_label = (
                str(df_label).strip().lower()
            )  # Normalize case and spaces

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
            value = df_view_vendaMagento_valor_status_final.iloc[
                i,
                df_view_vendaMagento_valor_status_final.columns.get_loc(
                    order_date
                ),
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
            f"✅ Successfully updated [df_view_vendaMagento_valor_status_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # # Debugging: Print mapped labels
    # print("📌 Google Sheets Rows:", row_labels)
    # print(
    #     "📌 DataFrame Row Labels:",
    #     [
    #         info_types.get(r, "❌ No Match")
    #         for r in df_view_vendaMagento_valor_status_final.iloc[
    #             :, 0
    #         ].tolist()
    #     ],
    # )

    # In[3]: Trazer valor de produto comprado e CMV para df_order

    # In[4]: orderItem

    #### Trazer dados da db de orderItem #####

    def fetch_order_items_batch(batch, client):
        """
        Fetches order items for a specific batch of order IDs.

        Args:
            batch (list): A batch of order IDs.
            client: The client making the request.

        Returns:
            List of fetched order items.
        """
        try:
            response = (
                supabase.table("OrderItem")
                .select("*")
                .order("orderId", desc=False)
                .in_("orderId", batch)
                .execute()
            )

            if response.data:
                return response.data
            return []

        except Exception as e:
            print(f"⚠️ Error fetching batch: {e}")
            return []

    def get_order_item(client, order_ids, batch_size=200, max_workers=5):
        """
        Fetches order item data for only the specified order IDs in parallel batches.

        Args:
            client: The client making the request.
            order_ids (list): A list of order IDs to filter the order items.
            batch_size (int): The number of order IDs per batch.
            max_workers (int): Number of parallel workers for faster processing.

        Returns:
            A DataFrame with order item data if available, otherwise None.
        """
        try:
            total_batches = math.ceil(len(order_ids) / batch_size)
            batches = [
                order_ids[i * batch_size : (i + 1) * batch_size]
                for i in range(total_batches)
            ]
            all_data = []

            print(
                f"🔄 Fetching {len(order_ids)} order items in {total_batches} batches..."
            )

            # Use multithreading to fetch batches in parallel
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                results = executor.map(
                    fetch_order_items_batch, batches, [client] * total_batches
                )

            # Collect results
            for result in results:
                all_data.extend(result)

            return pd.DataFrame(all_data) if all_data else None

        except Exception as e:
            print(f"❌ Error fetching orderItem data: {e}")
            return None

    # Get the unique order IDs from df_order
    order_ids = df_order["order_id"].unique().tolist()

    # Fetch the filtered order items
    try:
        df_order_item = get_order_item(
            client, order_ids, batch_size=200, max_workers=5
        )  # Fetch in parallel batches
        if df_order_item is not None:
            print(f"✅ Successfully fetched {len(df_order_item)} order items!")
        else:
            print(f"⚠️ No order item data found for client: {client}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # In[5]: ProductPrice

    def fetch_product_prices_batch(batch, client):
        """
        Fetches product prices for a specific batch of product IDs.

        Args:
            batch (list): A batch of product IDs.
            client: The client making the request.

        Returns:
            List of fetched product prices.
        """
        try:
            response = (
                supabase.table("ProductPriceLinx")
                .select("*")
                .order("model_id", desc=False)  # model_id in the database
                .in_(
                    "model_id", batch
                )  # Filtering by model_id from df_order_item
                .execute()
            )

            if response.data:
                return response.data
            return []

        except Exception as e:
            print(f"⚠️ Error fetching batch: {e}")
            return []

    def get_product_prices(client, product_ids, batch_size=200, max_workers=5):
        """
        Fetches product price data for only the specified product IDs in parallel batches.

        Args:
            client: The client making the request.
            product_ids (list): A list of product IDs (from df_order_item).
            batch_size (int): The number of product IDs per batch.
            max_workers (int): Number of parallel workers for faster processing.

        Returns:
            A DataFrame with product price data if available, otherwise None.
        """
        try:
            total_batches = math.ceil(len(product_ids) / batch_size)
            batches = [
                product_ids[i * batch_size : (i + 1) * batch_size]
                for i in range(total_batches)
            ]
            all_data = []

            print(
                f"🔄 Fetching {len(product_ids)} product prices in {total_batches} batches..."
            )

            # Use multithreading to fetch batches in parallel
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                results = executor.map(
                    fetch_product_prices_batch,
                    batches,
                    [client] * total_batches,
                )

            # Collect results
            for result in results:
                all_data.extend(result)

            return pd.DataFrame(all_data) if all_data else None

        except Exception as e:
            print(f"❌ Error fetching productPrice data: {e}")
            return None

    # Get the unique product IDs from df_order_item (mapped to model_id in the database)
    df_order_item["model_id"] = df_order_item["product_sku"].str.extract(
        r"^([^\.]+)"
    )

    product_ids = df_order_item["model_id"].astype(str).unique().tolist()

    # Fetch the filtered product prices
    try:
        df_product_price = get_product_prices(
            client, product_ids, batch_size=200, max_workers=5
        )  # Fetch in parallel batches
        if df_product_price is not None:
            print(
                f"✅ Successfully fetched {len(df_product_price)} product prices!"
            )
        else:
            print(f"⚠️ No product price data found for client: {client}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # In[6]: Trazer custo de produto para df_order_item

    df_order_item = df_order_item.merge(
        df_product_price,
        left_on="model_id",  # Column in df_order_item
        right_on="model_id",  # Column in df_product_price (database model_id)
        how="left",  # Keep all rows from df_order_item, even if there's no matching price
    )

    # In[7]: Calcular por pedido, valor de produto total liquido e CMV

    # Calcular valor produto liquido
    df_order_item["product_order_total_value"] = (
        df_order_item["product_order_price"]
        * df_order_item["product_order_qty"]
    ) - df_order_item["product_order_discount"]

    # Calcular CMV
    df_order_item["product_order_total_cost"] = (
        df_order_item["cost"] * df_order_item["product_order_qty"]
    )

    # Groupby por pedido
    df_order_item_info = df_order_item.groupby("orderId", as_index=False).agg(
        product_order_total_value=("product_order_total_value", "sum"),
        product_order_total_cost=("product_order_total_cost", "sum"),
    )

    # Trazer valor produto liquido e cmv para df_order
    df_order = df_order.merge(
        df_order_item_info, left_on="order_id", right_on="orderId", how="left"
    )

    df_order.fillna(0, inplace=True)

    # In[4]: Tratar dados para criar view vendas magento com indicadores de venda

    # Filter only 'processing' and 'complete' orders
    df_order_statusPaid = df_order[
        df_order["order_status"].isin(["processing", "complete"])
    ]

    # Filter only 'complete' orders
    complete_orders = df_order_statusPaid[
        df_order_statusPaid["order_status"] == "complete"
    ].index

    # Group by order_create_date_brazil to calculate metrics per date
    df_view_vendaMagento_info = (
        df_order_statusPaid.groupby("order_create_date_brazil")
        .agg(
            avg_ticket_value=(
                "order_total_value",
                "mean",
            ),
            total_order_credit_value=("order_credit_value", "sum"),
            qty_of_orders_paid=("order_id", "count"),
            total_product_order_value=("product_order_total_value", "sum"),
            total_order_product_qty=("order_product_qty", "sum"),
            total_product_order_cost=("product_order_total_cost", "sum"),
            qty_of_orders_complete=(
                "order_status",
                lambda x: (x == "complete").sum(),
            ),
            qty_of_products_complete=(
                "order_product_qty",
                lambda x: x[x.index.isin(complete_orders)].sum(),
            ),
        )
        .reset_index()
    )

    # ✅ Compute "avg_product_sold_value" and "product_order_mkp" separately
    df_view_vendaMagento_info["avg_product_sold_value"] = (
        df_view_vendaMagento_info["total_product_order_value"]
        / df_view_vendaMagento_info["total_order_product_qty"]
    )

    df_view_vendaMagento_info["product_order_mkp"] = (
        df_view_vendaMagento_info["total_product_order_value"]
        - df_view_vendaMagento_info["total_order_credit_value"]
    ) / df_view_vendaMagento_info["total_product_order_cost"]

    df_view_vendaMagento_info["avg_product_sold_qty_per_ticket"] = (
        df_view_vendaMagento_info["total_order_product_qty"]
        / df_view_vendaMagento_info["qty_of_orders_paid"]
    )

    # ✅ Drop intermediate columns used for calculations
    df_view_vendaMagento_info.drop(
        columns=[
            "total_product_order_value",
            "total_order_product_qty",
            "total_order_credit_value",
            # "qty_of_orders_paid",
        ],
        inplace=True,
    )

    df_view_vendaMagento_info = df_view_vendaMagento_info.set_index(
        "order_create_date_brazil"
    )

    df_view_vendaMagento_info_final = df_view_vendaMagento_info.T

    # In[7]: Save df_view_vendaMagento_info_final in google sheets

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
    df_view_vendaMagento_info_final = (
        df_view_vendaMagento_info_final.reset_index()
    )

    # Step 6: Define row mappings (info types)
    info_types = {
        "Ticket Médio": "avg_ticket_value",
        "Preço Médio": "avg_product_sold_value",
        "Peças por Atendimento": "avg_product_sold_qty_per_ticket",
        "Markup Vendas": "product_order_mkp",
        "Qtde Pedidos Faturado Magento": "qty_of_orders_complete",
        "Qtde Peças Faturado Magento": "qty_of_products_complete",
        "CMV": "total_product_order_cost",
        "Venda Magento (Geral)(apenas pagos)(qtd pedidos)": "qty_of_orders_paid",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_view_vendaMagento_info_final.columns[
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
            df_view_vendaMagento_info_final.iloc[:, 0]
        ):
            df_label = (
                str(df_label).strip().lower()
            )  # Normalize case and spaces

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
            value = df_view_vendaMagento_info_final.iloc[
                i,
                df_view_vendaMagento_info_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_view_vendaMagento_info_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[1]: Bater no db e pegar lista de pedidos

    def get_summary_order(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("SummaryOrder")
                    .select("*")
                    .order(
                        "order_create_date_brazil", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("order_create_date_brazil", start_date)
                    .lte("order_create_date_brazil", end_date)
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
            print(f"Error fetching SummaryOrder data for client {client}: {e}")
            raise

    rows_order = get_summary_order(client)
    if rows_order:
        df_summaryOrder = pd.DataFrame(rows_order)
    else:
        print(f"No SummaryOrder data found for client: {client}")

    # Organizar df para view final

    df_summaryOrder_value_final = df_summaryOrder.pivot_table(
        index="product_discount_range",  # Keep this as rows
        columns="order_create_date_brazil",  # Move this to columns
        values="order_item_value_total",  # Values to fill the table
        aggfunc="sum",  # Ensure aggregation if needed
    ).reset_index()  # Keep 'product_discount_range' as a column

    df_summaryOrder_value_final.fillna(0, inplace=True)

    df_summaryOrder_value_final.rename(
        columns={"product_discount_range": "index"}, inplace=True
    )

    df_summaryOrder_qty_final = df_summaryOrder.pivot_table(
        index="product_discount_range",  # Keep this as rows
        columns="order_create_date_brazil",  # Move this to columns
        values="order_item_qty_total",  # Values to fill the table
        aggfunc="sum",  # Ensure aggregation if needed
    ).reset_index()  # Keep 'product_discount_range' as a column

    df_summaryOrder_qty_final.fillna(0, inplace=True)

    df_summaryOrder_qty_final.rename(
        columns={"product_discount_range": "index"}, inplace=True
    )

    # In[7]: Save df_summaryOrder_value_final in google sheets

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
        "Venda Magento (ON)(R$)": "V: 0%",
        "Venda Magento (20%OFF)(R$)": "V: 20%",
        "Venda Magento (30%OFF)(R$)": "V: 30%",
        "Venda Magento (40%OFF)(R$)": "V: 40%",
        "Venda Magento (50%OFF)(R$)": "V: 50%",
        "Venda Magento (50%OFF +)(R$)": "V: 50% +",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_summaryOrder_value_final.columns[
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
        for i, df_label in enumerate(df_summaryOrder_value_final.iloc[:, 0]):
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
            value = df_summaryOrder_value_final.iloc[
                i,
                df_summaryOrder_value_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_summaryOrder_value_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[7]: Save df_summaryOrder_qty_final in google sheets

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
        "Venda Magento (ON)(qtd)": "V: 0%",
        "Venda Magento (20%OFF)(qtd)": "V: 20%",
        "Venda Magento (30%OFF)(qtd)": "V: 30%",
        "Venda Magento (40%OFF)(qtd)": "V: 40%",
        "Venda Magento (50%OFF)(qtd)": "V: 50%",
        "Venda Magento (50%OFF +)(qtd)": "V: 50% +",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_summaryOrder_qty_final.columns[
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
        for i, df_label in enumerate(df_summaryOrder_qty_final.iloc[:, 0]):
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
            value = df_summaryOrder_qty_final.iloc[
                i,
                df_summaryOrder_qty_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_summaryOrder_qty_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[4]: Tratar dados para criar view vendas magento por cupom

    # Filter only 'processing' and 'complete' orders
    df_order_statusPaid = df_order[
        df_order["order_status"].isin(["processing", "complete"])
    ]

    # Group by order_create_date_brazil to calculate metrics per date (cupom type)
    df_view_vendaMagento_coupon = (
        df_order_statusPaid.groupby(
            ["order_create_date_brazil", "order_coupon_type"]
        )
        .agg(
            order_total_value=(
                "order_total_value",
                "sum",
            ),
        )
        .reset_index()
    )

    # Organizar df para view final

    df_view_vendaMagento_coupon_final = (
        df_view_vendaMagento_coupon.pivot_table(
            index="order_coupon_type",  # Keep this as rows
            columns="order_create_date_brazil",  # Move this to columns
            values="order_total_value",  # Values to fill the table
            aggfunc="sum",  # Ensure aggregation if needed
        ).reset_index()
    )  # Keep 'product_discount_range' as a column

    df_view_vendaMagento_coupon_final.fillna(0, inplace=True)

    df_view_vendaMagento_coupon_final.rename(
        columns={"order_coupon_type": "index"}, inplace=True
    )

    # Group by order_create_date_brazil to calculate metrics per date (cupom por influencer)
    df_view_vendaMagento_coupon = (
        df_order_statusPaid.groupby(
            ["order_create_date_brazil", "order_coupon_code"]
        )
        .agg(
            order_total_value=(
                "order_total_value",
                "sum",
            ),
        )
        .reset_index()
    )

    # Organizar df para view final

    # Filter only cupom influencer
    df_view_vendaMagento_coupon_influencer = df_order_statusPaid[
        df_order_statusPaid["order_coupon_type"].isin(["coupon_influencer"])
    ]

    df_view_vendaMagento_coupon_influencer_final = (
        df_view_vendaMagento_coupon_influencer.pivot_table(
            index="order_coupon_code",  # Keep this as rows
            columns="order_create_date_brazil",  # Move this to columns
            values="order_total_value",  # Values to fill the table
            aggfunc="sum",  # Ensure aggregation if needed
        ).reset_index()
    )  # Keep 'product_discount_range' as a column

    df_view_vendaMagento_coupon_influencer_final.fillna(0, inplace=True)

    df_view_vendaMagento_coupon_influencer_final.rename(
        columns={"order_coupon_code": "Nome Influencer"}, inplace=True
    )

    # In[7]: Save df_view_vendaMagento_coupon_final in google sheets

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
        "Venda Cupom (Outros)": "coupon_outros",
        "Venda Cupom (Especial do Dia)": "coupon_campanha",
        "Venda Cupom (Influencers)": "coupon_influencer",
        "Venda Cupom (Vendedores)": "coupon_vendedor",
        "Venda (Cashback)": "coupon_cashback",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_view_vendaMagento_coupon_final.columns[
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
            df_view_vendaMagento_coupon_final.iloc[:, 0]
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
            value = df_view_vendaMagento_coupon_final.iloc[
                i,
                df_view_vendaMagento_coupon_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_view_vendaMagento_coupon_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[7]: Save df_view_vendaMagento_coupon_influencer_final in google sheets

    from gspread.utils import rowcol_to_a1

    gc = gspread.oauth()
    sh = gc.open(
        f"{dic_nomes[client]} - Relatório Geral E-Commerce"
    ).worksheet("Cupom Influencer")

    # Get all values from Google Sheets (before appending)
    data = sh.get_all_values()
    df_sheet = pd.DataFrame(data)

    # Extract column headers (first row assumed to be headers)
    date_columns = [
        str(date).strip() for date in df_sheet.iloc[1, 1:].tolist()
    ]
    influencer_names = [
        str(name).strip().lower() for name in df_sheet.iloc[1:, 0].tolist()
    ]

    updates = []
    new_rows = []

    # Iterate through influencers in the DataFrame
    for index, row in df_view_vendaMagento_coupon_influencer_final.iterrows():
        influencer_name = str(row.get("Nome Influencer", "")).strip().lower()
        if influencer_name in influencer_names:
            row_idx = (
                influencer_names.index(influencer_name) + 2
            )  # Adjust for headers
        else:
            # If not found, append to Google Sheets
            row_idx = len(influencer_names) + 2
            influencer_names.append(influencer_name)
            new_rows.append([influencer_name] + [""] * len(date_columns))

    # ✅ Step 1: Append new influencer rows if needed
    if new_rows:
        sh.append_rows(new_rows)
        print(f"✅ Added {len(new_rows)} new influencers to Google Sheets.")

        # ✅ Step 2: Refresh Google Sheets data (to include new rows)
        data = sh.get_all_values()
        df_sheet = pd.DataFrame(data)

        # ✅ Step 3: Recompute row labels (First column)
        influencer_names = [
            str(name).strip().lower() for name in df_sheet.iloc[1:, 0].tolist()
        ]

    # ✅ Step 4: Proceed with batch updating values (after refreshing row labels)
    for order_date in df_view_vendaMagento_coupon_influencer_final.columns[
        1:
    ]:  # Skip first column
        order_date_str = pd.to_datetime(order_date, errors="coerce").strftime(
            "%Y-%m-%d"
        )

        if order_date_str not in date_columns:
            print(f"⚠️ Date {order_date_str} not found in Google Sheets!")
            continue

        col_idx = date_columns.index(order_date_str) + 2  # Adjust for headers

        for (
            index,
            row,
        ) in df_view_vendaMagento_coupon_influencer_final.iterrows():
            influencer_name = (
                str(row.get("Nome Influencer", "")).strip().lower()
            )
            if influencer_name in influencer_names:
                row_idx = (
                    influencer_names.index(influencer_name) + 2
                )  # Convert to 1-based index
            else:
                print(
                    f"⚠️ Row for '{influencer_name}' not found in Google Sheets!"
                )
                continue  # Skip if row label is not found

            # ✅ Get value from DataFrame
            value = row[order_date]

            # ✅ Add batch update
            updates.append(
                {
                    "range": rowcol_to_a1(row_idx, col_idx),
                    "values": [[value]],
                }
            )

    # ✅ Step 5: Execute batch update in Google Sheets
    if updates:
        sh.batch_update(updates)
        print(
            f"✅ Successfully updated {len(updates)} values in Google Sheets!"
        )
    else:
        print("⚠️ No updates were made. Check for missing matches.")

    # In[4]: Bater no db e trazer stock summary

    def get_summary_stock(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("SummaryStock")
                    .select("*")
                    .order(
                        "stock_date", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .gte("stock_date", dataname4)
                    .lte("stock_date", dataname1)
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
            print(f"Error fetching stock data for client {client}: {e}")
            raise

    try:
        rows_stock = get_summary_stock(client)
        if rows_stock:
            df_summaryStock = pd.DataFrame(rows_stock)
        else:
            print(f"No stock data found for client: {client}")

    except Exception as e:
        print(e)

    # Organizar df para view final

    df_summaryStock_value_final = df_summaryStock.pivot_table(
        index="product_discount_range",  # Keep this as rows
        columns="stock_date",  # Move this to columns
        values="stock_value_total",  # Values to fill the table
        aggfunc="sum",  # Ensure aggregation if needed
    ).reset_index()  # Keep 'product_discount_range' as a column

    df_summaryStock_value_final.fillna(0, inplace=True)

    df_summaryStock_value_final.rename(
        columns={"product_discount_range": "index"}, inplace=True
    )

    df_summaryStock_qty_final = df_summaryStock.pivot_table(
        index="product_discount_range",  # Keep this as rows
        columns="stock_date",  # Move this to columns
        values="stock_qty_total",  # Values to fill the table
        aggfunc="sum",  # Ensure aggregation if needed
    ).reset_index()  # Keep 'product_discount_range' as a column

    df_summaryStock_qty_final.fillna(0, inplace=True)

    df_summaryStock_qty_final.rename(
        columns={"product_discount_range": "index"}, inplace=True
    )

    df_summaryStock_qty_final.iloc[:, 1:] = df_summaryStock_qty_final.iloc[
        :, 1:
    ].astype(int)

    # print(df_summaryStock_qty_final.dtypes)

    # In[7]: Save df_summaryStock_value_final in google sheets

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
        "MAGENTO: Poder de Venda (ON)": "E: 0%",
        "MAGENTO: Poder de Venda (20%OFF)": "E: 20%",
        "MAGENTO: Poder de Venda (30%OFF)": "E: 30%",
        "MAGENTO: Poder de Venda (40%OFF)": "E: 40%",
        "MAGENTO: Poder de Venda (50%OFF)": "E: 50%",
        "MAGENTO: Poder de Venda (50%OFF +)": "E: 50% +",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_summaryStock_value_final.columns[
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
        for i, df_label in enumerate(df_summaryStock_value_final.iloc[:, 0]):
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
            value = df_summaryStock_value_final.iloc[
                i,
                df_summaryStock_value_final.columns.get_loc(order_date),
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
            f"✅ Successfully updated [df_summaryStock_value_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )

    # In[7]: Save df_summaryStock_qty_final in google sheets

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
        "MAGENTO: Qtde Peças em Estoque (ON)": "E: 0%",
        "MAGENTO: Qtde Peças em Estoque (20%OFF)": "E: 20%",
        "MAGENTO: Qtde Peças em Estoque (30%OFF)": "E: 30%",
        "MAGENTO: Qtde Peças em Estoque (40%OFF)": "E: 40%",
        "MAGENTO: Qtde Peças em Estoque (50%OFF)": "E: 50%",
        "MAGENTO: Qtde Peças em Estoque (50%OFF +)": "E: 50% +",
    }

    # Step 7: Get normalized row labels from Google Sheets (First column)
    row_labels = [
        str(label).strip().lower() for label in df_sheet.iloc[:, 0].tolist()
    ]

    # Step 8: Prepare batch update list
    updates = []

    # Step 9: Iterate over the column headers (dates) in DataFrame
    for order_date in df_summaryStock_qty_final.columns[
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
        for i, df_label in enumerate(df_summaryStock_qty_final.iloc[:, 0]):
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
            value = df_summaryStock_qty_final.iloc[
                i,
                df_summaryStock_qty_final.columns.get_loc(order_date),
            ]

            # ✅ Add batch update
            updates.append(
                {
                    "range": f"{gspread.utils.rowcol_to_a1(row_idx, col_idx)}",
                    "values": [[value]],
                }
            )

    # Step 11: Convert all values in `updates` to JSON-serializable types before updating Google Sheets
    for update in updates:
        update["values"] = [
            [
                int(value)
                if isinstance(value, (np.integer, int))
                else float(value)
                if isinstance(value, (np.floating, float))
                else str(value)
                if isinstance(value, (np.object_, np.str_, str))
                else value
            ]
            for value in update["values"][0]
        ]  # Process each row's values properly

    # Step 12: Execute batch update in Google Sheets
    if updates:
        sh.batch_update(updates)
        print(
            f"✅ Successfully updated [df_summaryStock_qty_final] {len(updates)} values in Google Sheets!"
        )
    else:
        print(
            "⚠️ No updates were made. Check for missing matches between Google Sheets and DataFrame."
        )
