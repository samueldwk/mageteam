# Pegar da DB produtos em categoria do site

import pandas as pd
import date_functions as datef
from datetime import timedelta, date
from datetime import datetime
import dotenv
import os

dotenv.load_dotenv()

# DATE FUCTIONS

d1 = date.today() - timedelta(days=1)  # YESTERDAY DATE
# d1 = datetime(2025, 2, 8).date()  # SELECT DATE

datasql, datatxt, dataname1, dataname2, dataname3, dataname4 = datef.dates(d1)
# dataname4_date_format = datetime.strptime(dataname4, "%Y-%m-%d").date()
# dataname3_date_format = datetime.strptime(dataname3, "%Y-%m-%d").date()
# dataname2_date_format = datetime.strptime(dataname2, "%Y-%m-%d").date()
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
    datetime.combine(d1, datetime.min.time()) + timedelta(hours=3)
).isoformat()
end_date = (
    datetime.combine(d1, datetime.min.time())
    + timedelta(hours=3, seconds=86399)
).isoformat()

for client in c_list:
    try:
        # In[1]: Bater no db e pegar lista de produtos configuraveis e suas informaÃ§Ãµes

        def get_site_category_newin(client):
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
                    f"Error fetching NewInCatalog data for client {client}: {e}"
                )
                raise

        try:
            rows_catalog_newin = get_site_category_newin(client)
            if rows_catalog_newin:
                df_catalog_newin = pd.DataFrame(rows_catalog_newin)
            else:
                print(f"No NewInCatalog data found for client: {client}")

        except Exception as e:
            print(e)

        # In[1]: Bater no db e pegar lista de produtos configuraveis e suas informaÃ§Ãµes

        def get_site_catalog_sale(client):
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
                    f"Error fetching SaleCatalog data for client {client}: {e}"
                )
                raise

        try:
            rows_catalog_sale = get_site_catalog_sale(client)
            if rows_catalog_sale:
                df_catalog_sale = pd.DataFrame(rows_catalog_sale)
            else:
                print(f"No SaleCatalog data found for client: {client}")

        except Exception as e:
            print(e)

    except:
        print(f"*****ERRO: CATEGORIA SITE {client}: d1")
