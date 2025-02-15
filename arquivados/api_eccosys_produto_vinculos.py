import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time
import math


dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt, dataname, datasql, dataname2, dataname3, dataname4 = datef.dates(d1)

# CLIENT LIST
c_list = [
    "alanis",
    "basicler",
    # "dadri",
    "french",
    "haut",
    # "infini",
    "kle",
    "mun",
    "muna",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
    "uniquechic",
]

c_list = ["rery"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os produtos

    # Base URL for the API
    url_prod = "https://empresa.eccosys.com.br/api/produtos"
    count = 1000  # Number of products per request
    offset = 0  # Start from the first product

    # Initialize an empty list to store product data
    all_products = []

    while True:
        # Build the query string with dynamic offset
        params = {
            "$offset": offset,
            "$count": count,
            "$dataConsiderada": "data",
            "$situacao": "A",
            "$opcEcommerce": "S",
        }

        # Make the API request
        response_prod = requests.get(url_prod, params=params, headers=headers)

        # Handle errors or end of products
        if response_prod.status_code == 404:
            print("No more products found. Finishing loop.")
            break
        elif response_prod.status_code != 200:
            print(f"Error: {response_prod.status_code} - {response_prod.text}")
            break

        # Parse response JSON
        products = response_prod.json()
        # products = data.get("products", [])  # Adjust based on the API's response structure

        # Add products to the list
        all_products.extend(products)

        # Increment offset for the next request
        offset += count

    # Convert the list of products into a DataFrame
    df_ecco_produto = pd.DataFrame(all_products)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "nome",
        "codigo",
        "situacao",
        "idProdutoMaster",
    ]

    df_ecco_produto = df_ecco_produto[columns_to_keep]

    # Renomear os nomes das colunas
    df_ecco_produto.rename(
        columns={
            "id": "idProduto",
            "nome": "nomeProduto",
            "codigo": "codigoProduto",
            "situacao": "statusProduto",
            "idProdutoMaster": "idProdutoPai",
        },
        inplace=True,
    )

    # Colocar coluna de data cadastro
    df_ecco_produto["data"] = dataname

    # Colocar coluna mage_cliente
    df_ecco_produto["mage_cliente"] = client

    # Salvar df produto em excel
    # Specify the file name you want to save the Excel file as
    file_name = f"{client}: vinculos.xlsx"

    # Save the DataFrame to an Excel file
    df_ecco_produto.to_excel(file_name, index=False)

    # SALVAR VINCULOS

    directory = r"C:\Users\Samuel Kim\OneDrive\Documentos\mage_performance\Python\excel"

    file_name = f"{client}_vinculos.xlsx"

    # Define the full file path
    file_path = os.path.join(directory, file_name)

    # Save DataFrame to Excel file
    df_ecco_produto.to_excel(file_path, index=False)
