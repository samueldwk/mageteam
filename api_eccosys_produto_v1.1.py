# api_eccosys_produto_v1.1 gitactions
# api que consegue lidar com lista grande de produtos na hora de fazer upsert no database
# oficial desde 22/09/2024

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time
import math
from pushover_notification import send_pushover_notification

dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

# CLIENT LIST
c_list = [
    # "alanis",
    # "basicler",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    # "mun",
    # "muna",
    # "nobu",
    # "othergirls",
    # "rery",
    "talgui",
    # "paconcept",
    # "una",
    # "uniquechic",
]

# c_list = ["infini"]

# SUPABASE AUTH

from supabase import create_client, Client
import supabase

supabase_admin_user = os.environ.get("supabase_admin_user")
supabase_admin_password = os.environ.get("supabase_admin_password")

url: str = os.environ.get("SUPABASE_BI_URL")
key: str = os.environ.get("SUPABASE_BI_KEY")
supabase: Client = create_client(url, key)

# Autentificar usuário
auth_response = supabase.auth.sign_in_with_password(
    {"email": supabase_admin_user, "password": supabase_admin_password}
)

time.sleep(15)

# ECCOSYS API HEADER

payload = {}
files = {}

for client in c_list:
    try:
        headers = {
            "Authorization": os.getenv(f"ecco_aut_{client}"),
            "Content-Type": "application/json;charset=utf-8",
        }

        # In[1]: Eccosys API: GET Listar todos os produtos

        url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$situacao=A&$opcEcommerce=S"

        response_prod = requests.request(
            "GET", url_prod, headers=headers, data=payload
        )

        dic_ecco_produto = response_prod.json()
        df_ecco_produto = pd.DataFrame.from_dict(dic_ecco_produto)

        # COLUMNS TO KEEP
        columns_to_keep = [
            "id",
            "nome",
            "codigo",
            "preco",
            "situacao",
            "precoCusto",
            "idProdutoMaster",
            "precoDe",
        ]

        df_ecco_produto = df_ecco_produto[columns_to_keep]

        # Renomear os nomes das colunas
        df_ecco_produto.rename(
            columns={
                "id": "idProduto",
                "nome": "nomeProduto",
                "codigo": "codigoProduto",
                "preco": "precoProduto",
                "situacao": "statusProduto",
                "precoCusto": "precoCustoProduto",
                "idProdutoMaster": "idProdutoPai",
                "precoDe": "precoLancamentoProduto",
            },
            inplace=True,
        )

        # Colocar coluna de data cadastro
        df_ecco_produto["data"] = dataname

        # Colocar coluna mage_cliente
        df_ecco_produto["mage_cliente"] = client

        # # Salvar df produto em excel
        # # Specify the file name you want to save the Excel file as
        # file_name = "output.xlsx"

        # # Save the DataFrame to an Excel file
        # df_ecco_produto.to_excel(file_name, index=False)

        # In[11]: Enviar informações para DB

        dic_ecco_produto = df_ecco_produto.to_dict(orient="records")

        # Set batch size (e.g., 10000 records per batch)
        batch_size = 10000

        # Calculate how many batches we need
        num_batches = math.ceil(len(dic_ecco_produto) / batch_size)

        # Loop through the data and upsert it in batches
        for i in range(num_batches):
            # Create a batch by slicing the dictionary
            batch = dic_ecco_produto[i * batch_size : (i + 1) * batch_size]

            try:
                # Upsert this batch into the database
                response = (
                    supabase.table("mage_eccosys_produto_v1")
                    .upsert(batch, returning="minimal")
                    .execute()
                )
                print(
                    f"{client}: api_eccosys_produto_v1.1 Batch {i + 1}/{num_batches} inserted successfully."
                )
            except Exception as exception:
                print(
                    f"{client}: api_eccosys_produto_v1.1 Batch {i + 1} failed: {exception}"
                )

    except Exception as e:
        print(
            f"*****ERRO: {client} | api_eccosys_produto_v1.1 | {e} [{dataname}]"
        )
        send_pushover_notification(
            f"*****ERRO: {client} | api_eccosys_produto_v1.1 | {e} [{dataname}]"
        )
