# api_eccosys_produto_v1 gitactions
# substituido por api_eccosys_produto_v1.1 em 22/09/2024

import requests
import pandas as pd
import date_functions as datef
import datetime
import dotenv
import os
import time

dotenv.load_dotenv()

# DATE FUCTIONS
hj = datetime.datetime.now()
d1 = datef.dmenos(hj).date()

# #Para puxar de uma data específica

# d1 = datetime.datetime(2024, 6, 8).date()

datatxt, dataname, datasql, dataname2, dataname3 = datef.dates(d1)

# CLIENT LIST
c_list = [
    "alanis",
    # "dadri",
    "french",
    "haut",
    # "infini",
    "kle",
    "mun",
    "nobu",
    "othergirls",
    "rery",
    "talgui",
    "paconcept",
    "una",
    "uniquechic",
]

# c_list = ["french"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$opcEcommerce=S"

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

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_produto = df_ecco_produto.to_dict(orient="records")

    try:
        response = (
            supabase.table("mage_eccosys_produto_v1")
            .upsert(dic_ecco_produto)
            .execute()
        )

    except Exception as exception:
        print(f"{client}: {exception}")

    print(f"{client}: api_eccosys_produto_v1")
