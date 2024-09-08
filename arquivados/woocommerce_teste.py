# WooCommerce API: Teste inicial

# from woocommerce import API
import dotenv
import os
import datetime
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from woocommerce import API


dotenv.load_dotenv()

# # Parâmetros iniciais
store_url = "https://takeon.com.br"
consumer_key = os.getenv("woocommerce_consumer_key")
consumer_secret = os.getenv("woocommerce_consumer_secret")

wcapi = API(
    url=store_url,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    version="wc/v3",
)


# In[01]: GET Products

# URL Products
url_products = f"{store_url}/wp-json/wc/v3/products?per_page=10"

# Fazer o request de get de products
response_products = requests.get(
    url_products, auth=HTTPBasicAuth(consumer_key, consumer_secret)
)

# Arrumar get de products
dic_woo_products = response_products.json()
df_woo_products = pd.DataFrame.from_dict(dic_woo_products)
# print(f"Response dic_woo_produtos: {dic_woo_products}")

# Lidar com próximas páginas do response
while "next" in response_products.links.keys():
    url_products_next = response_products.links.get("next").get("url")
    print(url_products_next)

    response_products = requests.get(
        url_products_next, auth=HTTPBasicAuth(consumer_key, consumer_secret)
    )

    dic_woo_products_next = response_products.json()
    df_woo_products_next = pd.DataFrame.from_dict(dic_woo_products_next)

    df_woo_products = pd.concat(
        [df_woo_products, df_woo_products_next], ignore_index=True
    )


# In[01]: GET Products (com date modified)

# Date from 3 days ago
d3 = (datetime.datetime.now() - datetime.timedelta(days=3)).isoformat()

# Get de Products com date modified a partir de 3 dias atrás
response_products_date = wcapi.get("products", params={"modified_after": d3})

# Arrumar get de products
dic_woo_products_date = response_products_date.json()
df_woo_products_date = pd.DataFrame.from_dict(dic_woo_products_date)
# print(f"Response dic_woo_produtos: {dic_woo_products_date}")

# Lidar com próximas páginas do response
while "next" in response_products_date.links.keys():
    url_products_date_next = response_products_date.links.get("next").get(
        "url"
    )
    print(url_products_date_next)

    response_products_date = requests.get(
        url_products_date_next,
        auth=HTTPBasicAuth(consumer_key, consumer_secret),
    )

    dic_woo_products_date_next = response_products_date.json()
    df_woo_products_date_next = pd.DataFrame.from_dict(
        dic_woo_products_date_next
    )

    df_woo_products_date = pd.concat(
        [df_woo_products_date, df_woo_products_date_next], ignore_index=True
    )

# Organizar df final

# Columns to keep
columns_to_keep = ["id", "name", "date_created", "date_modified"]
df_woo_products_date_final = df_woo_products_date[columns_to_keep]

# In[11]: Enviar informações para DB (woo_products_test)

from supabase import create_client, Client
import supabase

url: str = os.environ.get("SUPABASE_BI_URL")
key: str = os.environ.get("SUPABASE_BI_KEY")
supabase: Client = create_client(url, key)

dic_woo_products_date_final = df_woo_products_date_final.to_dict(
    orient="records"
)

try:
    response = (
        supabase.table("woo_products_test")
        .upsert(dic_woo_products_date_final)
        .execute()
    )

except Exception as exception:
    print(exception)

# In[03]: GET Product Variations

product_id = "2082"

# URL Products
url_product_variations = (
    f"{store_url}/wp-json/wc/v3/products/{product_id}/variations?per_page=10"
)

# Fazer o request de get de product variations
response_product_variations = requests.get(
    url_product_variations, auth=HTTPBasicAuth(consumer_key, consumer_secret)
)

# Arrumar get de product variations
dic_woo_product_variations = response_product_variations.json()
df_woo_product_variations = pd.DataFrame.from_dict(dic_woo_product_variations)
# print(f"Response dic_woo_produtos: {dic_woo_product_variations}")

# Lidar com próximas páginas do response
while "next" in response_product_variations.links.keys():
    url_product_variations_next = response_product_variations.links.get(
        "next"
    ).get("url")
    print(url_product_variations_next)

    response_product_variations = requests.get(
        url_product_variations_next,
        auth=HTTPBasicAuth(consumer_key, consumer_secret),
    )

    dic_woo_product_variations_next = response_product_variations.json()
    df_woo_product_variations_next = pd.DataFrame.from_dict(
        dic_woo_product_variations_next
    )

    df_woo_product_variations = pd.concat(
        [df_woo_product_variations, df_woo_product_variations_next],
        ignore_index=True,
    )
