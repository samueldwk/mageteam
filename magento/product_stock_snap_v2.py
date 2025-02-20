# lofty product_stock_snap, ler o estoque atual no final do dia por produto do db e gravar no banco um snap por faixa de desconto tirando produtos com problema no cadastro
# v2 tem a mudança das faixas de desconto

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime(2024, 11, 2).date()  # SELECT DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
# dataname4_date_format = datetime.strptime(dataname4, "%Y-%m-%d").date()
# dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
# dataname2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
dataname1_date_format = datetime.strptime(dataname1, "%Y-%m-%d").date()


# CLIENT LIST

c_list = ["lofty"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_LOFTY_URL")
key: str = os.environ.get("SUPABASE_LOFTY_KEY")
supabase: Client = create_client(url, key)

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
                            # print(
                            #     "✅ Less than limit fetched, stopping pagination."
                            # )
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

        # In[2]: Bater no db e pegar lista de produtos simples e suas informações

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
                        # print(
                        #     f"Retrieved {len(response.data)} rows"
                        # )  # Debugging
                        all_data.extend(response.data)
                        if len(response.data) < limit - 1:
                            # print(
                            #     "✅ Less than limit fetched, stopping pagination."
                            # )
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
            rows_simple = get_simple_product(client)
            if rows_simple:
                df_simple_product = pd.DataFrame(rows_simple)
            else:
                print(f"No simple_product data found for client: {client}")

        except Exception as e:
            print(e)

        # In[3]: Tratar dados de df_simple_product para pegar estoque a nível de configurable_product

        # Criar coluna de configurable_product_sku extraindo o texo de product_sku (texto antes do segundo ".")
        df_simple_product["configurable_product_sku"] = df_simple_product[
            "product_sku"
        ].str.extract(r"^([^\.]+\.[^\.]+)")

        # Agrupar estoque de product_sku a nível de configurable_product_sku
        df_product_stock = df_simple_product.groupby(
            "configurable_product_sku", as_index=False
        )["product_stock"].sum()

        # In[4]: Filtrar estoque de acordo com alguns status (status, em estoque, tem imagem, error, coisas que nao sao produto acabado...)

        # Trazer informações necessárias para filtrar produto disp. do df_configurable_product para df_product_stock
        df_configurable_product_selected = df_configurable_product[
            [
                "product_sku",
                "product_name",
                "product_price",
                "product_promo_price",
                "product_status",
                "product_image",
                "is_in_stock",
            ]
        ]

        df_product_stock_info = df_product_stock.merge(
            df_configurable_product_selected,
            left_on="configurable_product_sku",
            right_on="product_sku",
            how="left",
        )

        # Filtrar os produtos que podem estar disponivel no site
        df_product_stock_info["product_status"] = (
            df_product_stock_info["product_status"].astype(str).str.lower()
        )
        df_product_stock_info["product_image"] = (
            df_product_stock_info["product_image"].astype(str).str.lower()
        )
        df_product_stock_info["is_in_stock"] = (
            df_product_stock_info["is_in_stock"].astype(str).str.lower()
        )

        # Filter products that can be available on the site
        df_product_stock_disp = df_product_stock_info[
            (df_product_stock_info["product_status"] == "true")
            & (df_product_stock_info["product_image"] == "true")
            & (df_product_stock_info["is_in_stock"] == "true")
        ]

        # In[5]: Tratar dados dos produtos que poderiam estar disponivel no site para serem gravados na db

        # Calcular poder de venda
        df_product_stock_disp["product_stock_value"] = (
            df_product_stock_disp["product_stock"]
            * df_product_stock_disp["product_promo_price"]
        )

        # Categorizar produtos por faixa de desconto
        df_product_stock_disp["product_price_discount"] = 1 - (
            df_product_stock_disp["product_promo_price"]
            / df_product_stock_disp["product_price"]
        )

        precision = 5
        df_product_stock_disp[
            "product_price_discount"
        ] = df_product_stock_disp["product_price_discount"].round(precision)

        bins = [-float("inf"), 0, 0.2, 0.3, 0.4, 0.5, float("inf")]
        labels = [
            "E: 0%",
            "E: 20%",
            "E: 30%",
            "E: 40%",
            "E: 50%",
            "E: 50% +",
        ]

        df_product_stock_disp["product_discount_range"] = pd.cut(
            df_product_stock_disp["product_price_discount"],
            bins=bins,
            labels=labels,
            right=True,
        )

        result_discount_range = (
            df_product_stock_disp.groupby(
                "product_discount_range", observed=False
            )
            .agg(
                stock_qty_total=("product_stock", "sum"),
                stock_value_total=("product_stock_value", "sum"),
            )
            .reset_index()
        )

        df_stock_discount_range = pd.DataFrame(result_discount_range)

        # Selecionar apenas as colunas que serão gravados no db
        df_stock_discount_range = df_stock_discount_range[
            ["product_discount_range", "stock_qty_total", "stock_value_total"]
        ]

        # Adicionar a coluna de data do estoque
        df_stock_discount_range["stock_date"] = dataname1

        # In[6]: Gravar informações de df_product_stock_disp na db

        dic_stock_discount_range = df_stock_discount_range.to_dict(
            orient="records"
        )

        try:
            response = (
                supabase.table("SummaryStock")
                .upsert(dic_stock_discount_range, returning="minimal")
                .execute()
            )

        except Exception as e:
            print(e)

    except Exception as exception:
        print(f"*****ERRO: UPSERT product_stock_snap {client}: {exception}")
