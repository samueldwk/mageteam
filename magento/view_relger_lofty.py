# criar view do relatorio gerencial da lofty, puxando informaçoes da db

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os
from pushover_notification import send_pushover_notification
import ads_meta_lofty as fb
import gspread


dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime(2025, 2, 15).date()  # SELECT DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
# dataname4_date_format = datetime.strptime(dataname4, "%Y-%m-%d").date()
# dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
# dataname2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
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
    datetime.combine(d1, datetime.min.time()) + timedelta(hours=3)
).isoformat()
end_date = (
    datetime.combine(d1, datetime.min.time())
    + timedelta(hours=3, seconds=86399)
).isoformat()

for client in c_list:
    try:
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
                print(
                    f"No configurable_product data found for client: {client}"
                )

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
                        .order(
                            "orderId", desc=False
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
                    f"Error fetching orderItem data for client {client}: {e}"
                )
                raise

        try:
            rows_order_item = get_order_item(client)
            if rows_order_item:
                df_order_item = pd.DataFrame(rows_order_item)
            else:
                print(f"No order item data found for client: {client}")

        except Exception as e:
            print(e)

        # In[4]: view vendas totais

        # Calcular valor pago de credito por produto
        df_order["order_credit_value_per_product"] = (
            df_order["order_credit_value"] / df_order["order_product_qty"]
        )

        # Filtrar order por apenas status de complete e processing
        df_order_filtered = df_order[
            df_order["order_status"].isin(["processing", "complete"])
        ]

        # Soma total de pedidos aprovados
        relger_valor_pedidos_aprovados = df_order_filtered[
            "order_total_value"
        ].sum()

        # Soma de quantiadade de produtos comprados
        relger_qtd_produtos_comprados = df_order_filtered[
            "order_product_qty"
        ].sum()

        # Qtd de pedidos aprovados
        relger_qtd_pedidos_aprovados = df_order_filtered["order_id"].count()

        # Ticket médio
        relger_ticket_medio = (
            relger_valor_pedidos_aprovados / relger_qtd_pedidos_aprovados
        )

        # Preço médio
        relger_preco_medio = (
            relger_valor_pedidos_aprovados / relger_qtd_produtos_comprados
        )

        # In[4]: view vendas por faixa de desconto

        # Filtrar order_item de pedidos aprovados
        df_order_id_filtered = df_order_filtered["order_id"].unique()

        df_order_item_filtered = df_order_item[
            df_order_item["orderId"].isin(df_order_id_filtered)
        ]

        # Trazer order_credit_value_per_product de df_order para df_order_item
        df_order_selected = df_order[
            [
                "order_id",
                "order_credit_value_per_product",
            ]
        ]

        df_order_item_filtered_complete = df_order_item_filtered.merge(
            df_order_selected,
            left_on="orderId",
            right_on="order_id",
            how="left",
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
        bins = [-float("inf"), 0, 0.25, 0.45, 0.6, float("inf")]
        labels = [
            "V: <= 0%",
            "V: > 0% and <= 25%",
            "V: > 25% and <= 45%",
            "V: > 45% and <= 60%",
            "V: > 60%",
        ]

        # Add a new column 'product_discount_range' based on the bins
        df_order_item_filtered_info.loc[:, "product_discount_range"] = pd.cut(
            df_order_item_filtered_info["product_discount"],
            bins=bins,
            labels=labels,
        )

        # SUBSTITUTE ANY NAN BY 0 IN COLUMN DESCONTO POR FAIXA DE PREÇO
        df_order_item_filtered_info[
            "product_discount_range"
        ] = df_order_item_filtered_info["product_discount_range"].fillna(
            "V: <= 0%"
        )

        # Use groupby to sum 'product_order_price_paid' for each 'product_discount_range'
        result1 = (
            df_order_item_filtered_info.groupby(
                "product_discount_range", observed=False
            )["product_order_price_paid"]
            .sum()
            .reset_index()
        )

        # Create a new DataFrame 'rel_ger_discount_range_final' with the results
        df_rel_ger_discount_range = pd.DataFrame(result1)
        df_rel_ger_discount_range_final = df_rel_ger_discount_range.T

        df_rel_ger_discount_range_final = (
            df_rel_ger_discount_range_final.reset_index(drop=True)
        )

        df_rel_ger_discount_range_final.columns = (
            df_rel_ger_discount_range_final.iloc[0]
        )  # Set first row as columns
        df_rel_ger_discount_range_final = df_rel_ger_discount_range_final[
            1:
        ].reset_index(
            drop=True
        )  # Remove first row

        # In[4]: Bater no db e estoque

        def get_stock(client):
            try:
                all_data = []
                offset = 0
                limit = 1001  # Supabase API limit per request

                while True:
                    # print(
                    #     f"Fetching records from {offset} to {offset + limit - 1}"
                    # )  # Debugging
                    response = (
                        supabase.table("Summary")
                        .select("*")
                        .order(
                            "stock_date", desc=False
                        )  # Ensure consistent ordering
                        .range(offset, offset + limit - 1)  # Inclusive range
                        .gte("stock_date", dataname1)
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
                        # print("⚠️ No data returned, stopping pagination.")
                        break  # Stop if no data is returned

                return all_data if all_data else None

            except Exception as e:
                print(f"Error fetching stock data for client {client}: {e}")
                raise

        try:
            rows_stock = get_stock(client)
            if rows_stock:
                df_stock = pd.DataFrame(rows_stock)
            else:
                print(f"No stock data found for client: {client}")

        except Exception as e:
            print(e)

        # In[5]: Calcular valores de estoque

        # Soma total de qtd de estoque
        relger_stock_qty_total = df_stock["stock_qty_total"].sum()

        # Soma total de valor de estoque
        relger_stock_value_total = df_stock["stock_value_total"].sum()

        # Stock date magento
        relger_stock_date = df_stock["createdAt"].iloc[0]

        # Preço medio do estoque
        relger_stock_preco_medio = (
            relger_stock_value_total / relger_stock_qty_total
        )

        # In[6]: Criar view final que vai pro cliente de vendas e estoque

        # Store variables in a dictionary
        dic_relger_view_order_stock = {
            "relger_valor_pedidos_aprovados": [relger_valor_pedidos_aprovados],
            "relger_qtd_pedidos_aprovados": [relger_qtd_pedidos_aprovados],
            "relger_preco_medio": [relger_preco_medio],
            "relger_ticket_medio": [relger_ticket_medio],
            "relger_stock_date": [relger_stock_date],
            "relger_stock_qty_total": [relger_stock_qty_total],
            "relger_stock_value_total": [relger_stock_value_total],
            "relger_stock_preco_medio": [relger_stock_preco_medio],
        }

        # Convert dictionary to DataFrame
        df_relger_view_order_stock = pd.DataFrame(dic_relger_view_order_stock)

        df_relger_view_order_stock = df_relger_view_order_stock.reset_index(
            drop=True
        )

        # df relger stock por desconto

        df_stock_selected = df_stock[
            [
                "stock_value_total",
                "product_discount_range",
            ]
        ]

        # Ensure "product_discount_range" is in the columns
        df_stock_selected = df_stock_selected.reset_index()

        # Set "product_discount_range" as column labels and aggregate values properly
        df_stock_selected = df_stock_selected.pivot_table(
            index=None,  # We want a single row output
            columns="product_discount_range",
            values="stock_value_total",
            aggfunc="sum",  # Ensure values are summed into a single row
        )

        # Remove unnecessary column names
        df_stock_selected.columns.name = None

        df_stock_selected.reset_index(drop=True, inplace=True)

        # df relger date
        df_relger_view_date = pd.DataFrame(
            {"Data": [datatxt]}
        )  # Wrap in list if it's a string

        df_relger_view_date["Data"] = datatxt
        df_relger_view_date["Mês"] = pd.to_datetime(
            df_relger_view_date["Data"], dayfirst=True
        ).dt.month
        df_relger_view_date["Ano"] = pd.to_datetime(
            df_relger_view_date["Data"], dayfirst=True
        ).dt.year

        df_relger_view_date.fillna(0, inplace=True)

        # Juntar agora com df de venda por range de desconto
        df_relger_view_order_stock_final = pd.concat(
            [
                df_relger_view_date,
                df_relger_view_order_stock,
                df_rel_ger_discount_range_final,
                df_stock_selected,
            ],
            axis=1,
        )

        # In[7]: ADS Meta

        # Em casos quando o cliente ainda não tem fb, tentar bater no fb mas se nao der apenas continuar
        try:
            df_rel_ger_fb = fb.download_fb(client, dataname1)

            # CONCAT 'df_rel_ger_ecco', 'df_rel_ger_fb'
            df_relger_view_magento_fb = pd.concat(
                [df_relger_view_order_stock_final, df_rel_ger_fb], axis=1
            )

        except Exception:
            print(f"Erro ao tentar acessar dados do fb do cliente: {client}")
            df_relger_view_magento_fb = df_relger_view_order_stock_final.copy()
            pass

        df_relger_view_magento_fb = df_relger_view_magento_fb.drop(
            columns=["ads_meta_date"]
        )

        df_relger_view_magento_fb = df_relger_view_magento_fb.fillna(
            "esperando atualizacao"
        )

        # In[7]: Save in google sheets

        gc = gspread.oauth()

        sh = gc.open(
            f"{dic_nomes[client]} - Relat�rio Gerencial E-Commerce"
        ).worksheet("Di�rio")

        dic_relger_view_magento_fb = df_relger_view_magento_fb.values.tolist()

        print(f"{dic_nomes[client]} - Relat�rio Gerencial E-Commerce")

        sh.append_rows(dic_relger_view_magento_fb, table_range="A1")

    except Exception as exception:
        print(f"*****ERRO: VIEW {client}: d1")
