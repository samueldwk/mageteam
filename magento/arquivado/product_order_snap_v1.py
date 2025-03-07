# lofty product_order_snap, ler order do db e gravar no banco um snap por faixa de desconto tirando produtos com problema no cadastro

# criar view do relatorio gerencial da lofty, puxando informaçoes da db

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
d8 = date.today() - timedelta(days=7)  # yesterday -7
# d1 = datetime(2025, 2, 15).date()  # SELECT DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
dataname4_date_format = datetime.strptime(dataname4, "%Y-%m-%d").date()
dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
dataname2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()


# CLIENT LIST

c_list = ["lofty"]

# DICIONÁRIO DE NOMES

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
    datetime.combine(d8, datetime.min.time()) + timedelta(hours=3)
).isoformat()
end_date = (
    datetime.combine(d1, datetime.min.time())
    + timedelta(hours=3, seconds=86399)
).isoformat()

for client in c_list:
    # In[1]: Bater no db e pegar lista de produtos configuraveis e suas informações

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
                    # print("⚠️ No data returned, stopping pagination.")
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(
                f"Error fetching configurable_product data for client {client}: {e}"
            )
            raise

    try:
        rows_configurable = get_configurable_product(client)
        if rows_configurable:
            df_configurable_product = pd.DataFrame(rows_configurable)
        else:
            print(f"No configurable_product data found for client: {client}")

    except Exception as e:
        print(e)

    # In[2]: Bater no db e pegar lista de pedidos

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
                    # print("⚠️ No data returned, stopping pagination.")
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(f"Error fetching order data for client {client}: {e}")
            raise

    try:
        rows_order = get_order(client)
        if rows_order:
            df_order = pd.DataFrame(rows_order)
        else:
            print(f"No order data found for client: {client}")

    except Exception as e:
        print(e)

    # In[3]: Bater no db e pegar produto de pedido

    def get_order_item(client):
        try:
            all_data = []
            offset = 0
            limit = 1001  # Supabase API limit per request

            while True:
                # print(
                #     f"Fetching records from {offset} to {offset + limit - 1}"
                # )  # Debugging
                response = (
                    supabase.table("OrderItem")
                    .select("*")
                    .order("orderId", desc=False)  # Ensure consistent ordering
                    .range(offset, offset + limit - 1)  # Inclusive range
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
                    # print("⚠️ No data returned, stopping pagination.")
                    break  # Stop if no data is returned

            return all_data if all_data else None

        except Exception as e:
            print(f"Error fetching orderItem data for client {client}: {e}")
            raise

    try:
        rows_order_item = get_order_item(client)
        if rows_order_item:
            df_order_item = pd.DataFrame(rows_order_item)
        else:
            print(f"No order item data found for client: {client}")

    except Exception as e:
        print(e)

    # In[4]: view vendas totais filtrado

    # Calcular valor pago de credito por produto
    df_order["order_credit_value_per_product"] = (
        df_order["order_credit_value"] / df_order["order_product_qty"]
    )

    # Filtrar order por apenas status de complete e processing
    df_order_filtered = df_order[
        df_order["order_status"].isin(["processing", "complete"])
    ]

    # In[4]: view vendas por faixa de desconto

    # Filtrar order_item de pedidos aprovados
    df_order_id_filtered = df_order_filtered["order_id"].unique()

    df_order_item_filtered = df_order_item[
        df_order_item["orderId"].isin(df_order_id_filtered)
    ]

    # Trazer order_credit_value_per_product e order_create_date de df_order para df_order_item
    df_order_selected = df_order[
        ["order_id", "order_credit_value_per_product", "order_create_date"]
    ]

    df_order_item_filtered_complete = df_order_item_filtered.merge(
        df_order_selected,
        left_on="orderId",
        right_on="order_id",
        how="left",
    )

    # Ensure order_create_date is in datetime format
    df_order_item_filtered_complete["order_create_date"] = pd.to_datetime(
        df_order_item_filtered_complete["order_create_date"], errors="coerce"
    )

    # Check again if conversion worked
    print(
        df_order_item_filtered_complete["order_create_date"].dtype
    )  # Should be datetime64[ns]

    # Convert to Brazil Time Zone (UTC-3) and extract only the date
    df_order_item_filtered_complete.loc[:, "order_create_date_brazil"] = (
        df_order_item_filtered_complete["order_create_date"]
        .dt.tz_localize("UTC")  # Set original timezone as UTC
        .dt.tz_convert("America/Sao_Paulo")  # Convert to Brazil time (UTC-3)
        .dt.date  # Extract only the date (YYYY-MM-DD)
    )

    # Calcular desconto do produto vendido e criar range de desconto

    # Criar coluna de configurable_product_sku extraindo o texo de product_sku (texto antes do segundo ".")
    df_order_item_filtered_complete[
        "configurable_product_sku"
    ] = df_order_item_filtered_complete["product_sku"].str.extract(
        r"^([^\.]+\.[^\.]+)"
    )

    # Trazer informações necessárias para filtrar produto disp. do df_configurable_product para df_order_item_filtered
    df_configurable_product_selected = df_configurable_product[
        [
            "product_sku",
            # "product_name",
            "product_price",
        ]
    ]

    # Trazer preço de lancamento do produto
    df_order_item_filtered_info = df_order_item_filtered_complete.merge(
        df_configurable_product_selected,
        left_on="configurable_product_sku",
        right_on="product_sku",
        how="left",
    )

    # Calcular desconto de produto
    df_order_item_filtered_info["product_discount"] = 1 - (
        df_order_item_filtered_info["product_order_price"]
        / df_order_item_filtered_info["product_price"]
    )

    # Calcular valor liquido vendido
    df_order_item_filtered_info["product_order_price_paid"] = (
        (
            df_order_item_filtered_info["product_order_price"]
            * df_order_item_filtered_info["product_order_qty"]
        )
        - df_order_item_filtered_info["product_order_discount"]
        - df_order_item_filtered_info["order_credit_value_per_product"]
    )

    # Round the 'Desconto' values to a reasonable precision
    precision = 5  # Adjust the precision as needed
    df_order_item_filtered_info.loc[
        :, "product_discount"
    ] = df_order_item_filtered_info["product_discount"].round(precision)

    # Create bins for the 'product_discount' column
    bins = [-float("inf"), 0, 0.2, 0.3, 0.4, 0.5, float("inf")]
    labels = [
        "V: 0%",
        "V: 20%",
        "V: 30%",
        "V: 40%",
        "V: 50%",
        "V: 50% +",
    ]

    # Add a new column 'product_discount_range' based on the bins
    df_order_item_filtered_info.loc[:, "product_discount_range"] = pd.cut(
        df_order_item_filtered_info["product_discount"],
        bins=bins,
        labels=labels,
        right=True,
    )

    # SUBSTITUTE ANY NAN BY 0 IN COLUMN DESCONTO POR FAIXA DE PREÇO
    df_order_item_filtered_info[
        "product_discount_range"
    ] = df_order_item_filtered_info["product_discount_range"].fillna("V: 0%")

    # ✅ Reset index before grouping to avoid mismatches
    df_order_item_filtered_info = df_order_item_filtered_info.reset_index(
        drop=True
    )

    # ✅ Ensure categorical consistency
    df_order_item_filtered_info[
        "product_discount_range"
    ] = df_order_item_filtered_info["product_discount_range"].astype(
        str
    )  # Convert categorical to string

    df_summary_order_discount_range = (
        df_order_item_filtered_info.groupby(
            ["product_discount_range", "order_create_date_brazil"],
            observed=False,
            as_index=False,
        )
        .agg(
            order_item_value_total=(
                "product_order_price_paid",
                "sum",
            ),  # Sum price paid per date
            order_item_qty_total=(
                "product_order_qty",
                "sum",
            ),  # Sum quantity ordered
        )
        .reset_index(drop=True)  # Ensure clean index
    )

    df_summary_order_discount_range[
        "order_create_date_brazil"
    ] = df_summary_order_discount_range["order_create_date_brazil"].astype(str)

    print(df_summary_order_discount_range["order_create_date_brazil"].dtype)

    # In[6]: Gravar informações de df_product_stock_disp na db

    dic_summary_order_discount_range = df_summary_order_discount_range.to_dict(
        orient="records"
    )

    try:
        response = (
            supabase.table("SummaryOrder")
            .upsert(dic_summary_order_discount_range, returning="minimal")
            .execute()
        )

    except Exception as e:
        print(e)
