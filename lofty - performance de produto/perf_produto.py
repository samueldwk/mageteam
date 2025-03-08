# Criar relatório de performance de produto

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

# Convert to GMT 0 by adding 3 hours

start_date = (
    datetime.combine(dx, datetime.min.time()) + timedelta(hours=3)
).isoformat()
end_date = (
    datetime.combine(d1, datetime.min.time())
    + timedelta(hours=3, seconds=86399)
).isoformat()

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
    # In[1]: Supabase requests def

    # ConfigurableProduct
    def get_configurable_product(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("ConfigurableProduct")
                    .select("*")
                    .order(
                        "product_id", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .execute()
                )

                if response.data:
                    all_data.extend(response.data)
                    if len(response.data) < limit - 1:
                        break  # Stop when fewer than 'limit' records are returned
                    offset += len(
                        response.data
                    )  # Move offset by actual rows received
                else:
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(f"Error fetching order data for client {client}: {e}")
            raise

    # SimpleProduct
    def get_simple_product(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("SimpleProduct")
                    .select("*")
                    .order(
                        "product_id", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .execute()
                )

                if response.data:
                    all_data.extend(response.data)
                    if len(response.data) < limit - 1:
                        break  # Stop when fewer than 'limit' records are returned
                    offset += len(
                        response.data
                    )  # Move offset by actual rows received
                else:
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(
                f"Error fetching SimpleProduct data for client {client}: {e}"
            )
            raise

    # ProductPriceLinx (product cost)
    def get_product_cost(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("ProductPriceLinx")
                    .select("*")
                    .order(
                        "model_id", desc=False
                    )  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
                    .execute()
                )

                if response.data:
                    all_data.extend(response.data)
                    if len(response.data) < limit - 1:
                        break  # Stop when fewer than 'limit' records are returned
                    offset += len(
                        response.data
                    )  # Move offset by actual rows received
                else:
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(f"Error fetching product_cost data for client {client}: {e}")
            raise

    # Order
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

    # OrderItem
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

    # In[2]: Base Df

    # ConfigurableProduct
    rows_configurable_product = get_configurable_product(client)
    if rows_configurable_product:
        df_base_configurable_product = pd.DataFrame(rows_configurable_product)
    else:
        print(f"No configurable product data found for client: {client}")

    # SimpleProduct
    rows_simple_product = get_simple_product(client)
    if rows_simple_product:
        df_base_simple_product = pd.DataFrame(rows_simple_product)
    else:
        print(f"No simple product data found for client: {client}")

    # Product Cost
    rows_product_cost = get_product_cost(client)
    if rows_product_cost:
        df_base_product_cost = pd.DataFrame(rows_product_cost)
    else:
        print(f"No product cost data found for client: {client}")

    # Order
    rows_order = get_order(client)
    if rows_order:
        df_base_order = pd.DataFrame(rows_order)
    else:
        print(f"No order data found for client: {client}")

    # Order Item
    order_ids = (
        df_base_order["order_id"].unique().tolist()
    )  # Get the unique order IDs from df_order

    try:
        df_base_order_item = get_order_item(
            client, order_ids, batch_size=200, max_workers=5
        )  # Fetch in parallel batches
        if df_base_order_item is not None:
            print(
                f"✅ Successfully fetched {len(df_base_order_item)} order items!"
            )
        else:
            print(f"⚠️ No order item data found for client: {client}")
    except Exception as e:
        print(f"❌ Error: {e}")

    # In[3]: Informação de Produto

    df_filtered_configurable_product = df_base_configurable_product.loc[
        :,
        [
            "product_model_id",
            "product_sku",
            "product_name",
            "product_registry_date",
            "product_image",
            "product_status",
            "is_in_stock",
            "product_price",
            "product_promo_price",
        ],
    ]  # Filtrar apenas linhas que vamos precisar

    df_filtered_configurable_product[
        "product_name"
    ] = df_filtered_configurable_product[
        "product_name"
    ].str.title()  # Arrumar formatacao do texto do nome

    # df_filtered_configurable_product["product_registry_date"] = pd.to_datetime(
    #     df_filtered_configurable_product["product_registry_date"]
    # ).dt.date  # tranformar para apenas data

    df_filtered_configurable_product["product_registry_date"] = pd.to_datetime(
        df_filtered_configurable_product["product_registry_date"]
    ).dt.strftime(
        "%Y-%m-%d"
    )  # Converts to 'YYYY-MM-DD' as a string

    df_filtered_configurable_product = df_filtered_configurable_product[
        df_filtered_configurable_product["product_model_id"].notnull()
    ]  # Drop rows where product_model is None

    df_final_configurable_product = pd.merge(
        df_filtered_configurable_product,
        df_base_product_cost[["model_id", "cost"]],
        how="left",
        left_on="product_model_id",
        right_on="model_id",
    ).drop(
        columns=["model_id", "product_model_id"]
    )  # Trazer custo para df de produtos

    # In[4]: Estoque e Grade

    df_filtered_simple_product = df_base_simple_product.loc[
        :,
        [
            "product_sku",
            "product_stock",
        ],
    ]  # Filtrar apenas linhas que vamos precisar

    df_filtered_simple_product = df_filtered_simple_product[
        df_filtered_simple_product["product_stock"] > 0
    ]  # Drop linhas de produtos com estoque menor ou igual a 0

    df_filtered_simple_product[
        "configurable_product_sku"
    ] = df_filtered_simple_product["product_sku"].str.extract(
        r"^([^\.]+\.[^\.]+)"
    )  # Criar coluna de configurable_product_sku extraindo o texo de product_sku (texto antes do segundo ".")

    df_final_stock = (
        df_filtered_simple_product.groupby("configurable_product_sku")
        .agg(
            product_stock_qty=("product_stock", "sum"),
            product_stock_grade=("configurable_product_sku", "count"),
        )
        .reset_index()
    )  # Qtd estoque total e grade por variante

    # In[5]: Vendas por semana

    df_base_order["order_create_date"] = pd.to_datetime(
        df_base_order["order_create_date"], errors="coerce"
    )  # Criar coluna order_create_date_brazil tranforando UTM 0 em UTM -3 e tirando horário

    df_base_order.loc[:, "order_create_date_brazil"] = (
        df_base_order["order_create_date"]
        .dt.tz_localize("UTC")  # Set original timezone as UTC
        .dt.tz_convert("America/Sao_Paulo")  # Convert to Brazil time (UTC-3)
        .dt.date  # Extract only the date (YYYY-MM-DD)
    )

    df_filtered_order = df_base_order.loc[
        :,
        [
            "order_id",
            "order_create_date_brazil",
            "order_status",
        ],
    ]  # Filtrar apenas linhas que vamos precisar

    df_filtered_order_item = df_base_order_item.loc[
        :,
        [
            "orderId",
            "product_sku",
            "product_order_qty",
        ],
    ]  # Filtrar apenas linhas que vamos precisar

    df_filtered_order_item = pd.merge(
        df_filtered_order_item,
        df_filtered_order,
        how="left",
        left_on="orderId",
        right_on="order_id",
    ).drop(
        columns=["orderId"]
    )  # Trazer data e status do pedido

    df_filtered_order_item = df_filtered_order_item[
        df_filtered_order_item["order_status"].isin(["complete", "processing"])
    ]  # Manter apenas pedidos completos ou em processamento

    df_filtered_order_item[
        "configurable_product_sku"
    ] = df_filtered_order_item["product_sku"].str.extract(
        r"^([^\.]+\.[^\.]+)"
    )  # Criar coluna de configurable_product_sku extraindo o texo de product_sku (texto antes do segundo ".")

    df_final_order = (
        df_filtered_order_item.groupby(
            ["configurable_product_sku", "order_create_date_brazil"]
        )["product_order_qty"].sum()
        # .unstack(fill_value=0)
        .reset_index()
    )  # Groupby por sku configuravel e data to pedido

    df_final_order["order_create_date_brazil"] = pd.to_datetime(
        df_final_order["order_create_date_brazil"]
    )
    df_final_order["week"] = df_final_order[
        "order_create_date_brazil"
    ].dt.to_period("W")

    df_final_order_week = (
        df_final_order.groupby(["week", "configurable_product_sku"])
        .agg(product_order_qty=("product_order_qty", "sum"))
        .reset_index()
    )

    df_final_order_week = (
        df_final_order_week.pivot_table(
            index="configurable_product_sku",
            columns="week",
            values="product_order_qty",
            aggfunc="sum",
        )
        .fillna(0)
        .reset_index()
    )  # Juntar vendas por semana

    # In[6]: Montar df final juntando todas

    df_perf_prod = (
        pd.merge(
            df_final_configurable_product,
            df_final_stock,
            how="left",
            left_on="product_sku",
            right_on="configurable_product_sku",
        )
        .drop(columns=["configurable_product_sku"])
        .fillna(0)
    )

    df_perf_prod = (
        pd.merge(
            df_perf_prod,
            df_final_order_week,
            how="left",
            left_on="product_sku",
            right_on="configurable_product_sku",
        )
        .drop(columns=["configurable_product_sku"])
        .fillna(0)
    )

    # In[7]: Atualizar view no google sheets

    from gspread.utils import rowcol_to_a1

    gc = gspread.oauth()
    sh = gc.open(f"{dic_nomes[client]} - Performance de Produto").worksheet(
        "Relatório"
    )

    # Get all values from Google Sheets (before appending)
    data = sh.get_all_values()
    df_sheet = pd.DataFrame(data)

    # Extract column headers (first row assumed to be headers)
    dados_columns = [
        str(date).strip() for date in df_sheet.iloc[1, 1:].tolist()
    ]
    product_sku = [
        str(name).strip().lower() for name in df_sheet.iloc[1:, 0].tolist()
    ]

    updates = []
    new_rows = []

    # Iterate through product_sku in the DataFrame
    for index, row in df_perf_prod.iterrows():
        product_sku = str(row.get("product_sku", "")).strip().lower()
        if product_sku in product_sku:
            row_idx = product_sku.index(product_sku) + 2  # Adjust for headers
        else:
            # If not found, append to Google Sheets
            row_idx = len(product_sku) + 2
            product_sku.append(product_sku)
            new_rows.append([product_sku] + [""] * len(dados_columns))

    # ✅ Step 1: Append new product_sku rows if needed
    if new_rows:
        sh.append_rows(new_rows)
        print(f"✅ Added {len(new_rows)} new product_sku to Google Sheets.")

        # ✅ Step 2: Refresh Google Sheets data (to include new rows)
        data = sh.get_all_values()
        df_sheet = pd.DataFrame(data)

        # ✅ Step 3: Recompute row labels (First column)
        product_sku = [
            str(name).strip().lower() for name in df_sheet.iloc[1:, 0].tolist()
        ]

    # ✅ Step 4: Proceed with batch updating values (after refreshing row labels)
    for dados in df_perf_prod.columns[1:]:  # Skip first column
        if dados not in dados_columns:
            print(f"⚠️ Date {dados_columns} not found in Google Sheets!")
            continue

        col_idx = dados_columns.index(dados) + 2  # Adjust for headers

        for (
            index,
            row,
        ) in df_perf_prod.iterrows():
            product_sku = str(row.get("product_sku", "")).strip().lower()
            if product_sku in product_sku:
                row_idx = (
                    product_sku.index(product_sku) + 2
                )  # Convert to 1-based index
            else:
                print(
                    f"⚠️ Row for '{product_sku}' not found in Google Sheets!"
                )
                continue  # Skip if row label is not found

            # ✅ Get value from DataFrame
            value = row[dados]

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
