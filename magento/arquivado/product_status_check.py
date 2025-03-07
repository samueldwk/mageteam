# Objetivo da view é verificar se os produtos que tem estoque (ERP), estão disponíveis no site e depois descobrir o motivo.

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
                "product_status",
                "product_image",
                "is_in_stock",
                "product_registry_date",
            ]
        ]

        df_product_stock_info = df_product_stock.merge(
            df_configurable_product_selected,
            left_on="configurable_product_sku",
            right_on="product_sku",
            how="left",
        )

        # In[4]: Bater no db e pegar lista de produtos no site (menu new in)

        def get_catalog_newin(client):
            try:
                all_data = []
                offset = 0
                limit = 1001  # Supabase API limit per request

                while True:
                    # print(
                    #     f"Fetching records from {offset} to {offset + limit - 1}"
                    # )  # Debugging
                    response = (
                        supabase.table("NewInCatalog")
                        .select("*")
                        .order(
                            "product_sku", desc=False
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
                    f"Error fetching NewInCatalog data for client {client}: {e}"
                )
                raise

        try:
            rows_newin_catalog = get_catalog_newin(client)
            if rows_newin_catalog:
                df_catalog_newin = pd.DataFrame(rows_newin_catalog)
            else:
                print(f"No NewInCatalog data found for client: {client}")

        except Exception as e:
            print(e)

        # In[5]: Bater no db e pegar lista de produtos no site (menu sale)

        def get_catalog_sale(client):
            try:
                all_data = []
                offset = 0
                limit = 1001  # Supabase API limit per request

                while True:
                    # print(
                    #     f"Fetching records from {offset} to {offset + limit - 1}"
                    # )  # Debugging
                    response = (
                        supabase.table("SaleCatalog")
                        .select("*")
                        .order(
                            "product_sku", desc=False
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
                    f"Error fetching SaleCatalog data for client {client}: {e}"
                )
                raise

        try:
            rows_sale_catalog = get_catalog_sale(client)
            if rows_sale_catalog:
                df_catalog_sale = pd.DataFrame(rows_sale_catalog)
            else:
                print(f"No SaleCatalog data found for client: {client}")

        except Exception as e:
            print(e)

        # In[6]: Bater no db e pegar lista de produtos no site (menu acessorios)

        def get_catalog_acessory(client):
            try:
                all_data = []
                offset = 0
                limit = 1001  # Supabase API limit per request

                while True:
                    # print(
                    #     f"Fetching records from {offset} to {offset + limit - 1}"
                    # )  # Debugging
                    response = (
                        supabase.table("AccessoryCatalog")
                        .select("*")
                        .order(
                            "product_sku", desc=False
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
                    f"Error fetching AccessoryCatalog data for client {client}: {e}"
                )
                raise

        try:
            rows_acessory_catalog = get_catalog_acessory(client)
            if rows_acessory_catalog:
                df_catalog_acessory = pd.DataFrame(rows_acessory_catalog)
            else:
                print(f"No SaleCatalog data found for client: {client}")

        except Exception as e:
            print(e)

        # In[7]: Criar view de status geral dos produtos

        # Filtrar de magento apenas produtos com estoque
        df_magento_product_withstock = df_product_stock_info[
            df_product_stock_info["product_stock"] != 0
        ]

        # Criar uma df com todos os produtos no site
        df_catalog_newin["catalog_status"] = "catalog_newin"
        df_catalog_sale["catalog_status"] = "catalog_sale"
        df_catalog_acessory["catalog_status"] = "catalog_acessory"

        # df_catalog_all = pd.merge(
        #     df_catalog_newin, df_catalog_sale, how="outer", on="product_sku"
        # )

        df_catalog_all = pd.merge(
            pd.merge(
                df_catalog_newin,
                df_catalog_sale,
                how="outer",
                on="product_sku",
            ),
            df_catalog_acessory,
            how="outer",
            on="product_sku",
        )

        selected_columns = [
            "product_sku",
            "catalog_status_x",
            "catalog_status_y",
            "catalog_status",
        ]

        df_catalog_all_filtered = df_catalog_all[selected_columns]

        df_catalog_all_filtered[
            "site_catalog_status"
        ] = "site_catalog_available"

        # Juntar magento com site (lista por sku)
        df_product_status = pd.merge(
            df_magento_product_withstock,
            df_catalog_all_filtered,
            how="left",
            on="product_sku",
        )

        # In[6]: Exportar planilhas para Excel

        # Set the directory where you want to save the file
        directory = r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"

        # Construct the full file path
        file_path = os.path.join(
            directory, f"Product Status Check_{client}_({dataname1}).xlsx"
        )

        # Export df df_product_status
        df_product_status.to_excel(file_path, index=False)

        print("Export Complete: Export df_product_status.xlsx")

        # # Criar view do cliente
        # view_magento_product_count_withstock = df_product_status[
        #     "product_sku"
        # ].count()
        # view_magento_product_stock_qty = df_product_status[
        #     "product_stock"
        # ].sum()
        # view_site_catalog_product_count = df_product_status[
        #     "catalog_status"
        # ].count()
        # view_site_catalog_product_stock_qty = df_product_status[
        #     df_product_status["catalog_status"] == "site_catalog_available"
        # ]["product_stock"].sum()

        # df_view_product_status = pd.DataFrame(
        #     {
        #         "Magento": [
        #             view_magento_product_count_withstock,
        #             view_magento_product_stock_qty,
        #         ],
        #         "Site": [
        #             view_site_catalog_product_count,
        #             view_site_catalog_product_stock_qty,
        #         ],
        #     },
        #     index=["qtd modelos", "qtd peças"],
        # )

    except Exception as exception:
        print(f"*****ERRO: product_status_check {client}: {exception}")
