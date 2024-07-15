# rel_ger_ecco_api_v1.0
# Relatório de cliente

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time

dotenv.load_dotenv()

# # DATE FUCTIONS
# hj = datetime.datetime.now()
# d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

# datatxt1, dataname1, datasql, dataname2 = datef.dates(d1)

# CLIENT LIST
c_list = [
    # "ajobrand",
    "alanis",
    # "dadri",
    "french",
    # "haverroth",
    "haut",
    # "infini",
    "kle",
    # "luvic",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    # "talgui",
    "paconcept",
    "una",
]

c_list = ["haut"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os produtos

    url_client = "https://empresa.eccosys.com.br/api/clientes?$offset=0&$count=20&cpf=00000000000&cnpj=00000000000000"
    response_client = requests.request(
        "GET", url_client, headers=headers, data=payload
    )

    dic_ecco_cliente = response_client.json()
    df_ecco_cliente = pd.DataFrame.from_dict(dic_ecco_cliente)

    # COLUMNS TO KEEP
    # columns_to_keep = [
    #     "id",
    #     "codigo",
    #     "precoCusto",
    #     "precoDe",
    #     "preco",
    #     "situacao",
    # ]

    # df_ecco_cliente = df_ecco_cliente[columns_to_keep]
