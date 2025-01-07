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
