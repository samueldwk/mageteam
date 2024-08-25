# mage_bi_estoque_v1

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
    # "alanis",
    # "dadri",
    "french",
    # "haut",
    # "infini",
    # "kle",
    "mun",
    # "nobu",
    # "othergirls",
    # "rery",
    # "talgui",
    # "paconcept",
    # "una",
    # "uniquechic",
]

# c_list = ["mun"]

# API HEADER

payload = {}
files = {}

for client in c_list:
    headers = {
        "Authorization": os.getenv(f"ecco_aut_{client}"),
        "Content-Type": "application/json;charset=utf-8",
    }

    # In[1]: Eccosys API: GET Listar quantidades em estoque

    url_estoque = "https://empresa.eccosys.com.br/api/estoques?&$offset=0&$count=10000000"

    response_estoque = requests.request(
        "GET", url_estoque, headers=headers, data=payload, files=files
    )

    contador = 0

    while response_estoque.status_code != 200:
        print(
            f"{client}: Status_code response = {response_estoque.status_code}"
        )

        time.sleep(300)

        contador = contador + 1

        response_estoque = requests.request(
            "GET", url_estoque, headers=headers, data=payload, files=files
        )

        if contador == 60:
            print(f"{client}: request url_estoque reached try limit")

            break

    dic_ecco_estoque = response_estoque.json()
    df_ecco_estoque_cru = pd.DataFrame.from_dict(dic_ecco_estoque)

    # SUBSTITUTE ALL NEGATIVE VALUES BY 0
    df_ecco_estoque_limpo = df_ecco_estoque_cru.copy()

    df_ecco_estoque_limpo["estoqueReal"] = df_ecco_estoque_limpo[
        "estoqueReal"
    ].apply(lambda x: x if x > 0 else 0)

    df_ecco_estoque_limpo["estoqueDisponivel"] = df_ecco_estoque_limpo[
        "estoqueDisponivel"
    ].apply(lambda x: x if x > 0 else 0)

    # Colocar coluna de data estoque
    df_ecco_estoque_limpo["Data"] = dataname1

    # Tranformar formato coluna estoque para int
    columns_to_convert = ["estoqueDisponivel", "estoqueReal"]

    df_ecco_estoque_limpo[columns_to_convert] = df_ecco_estoque_limpo[
        columns_to_convert
    ].astype(int)

    # Renomear os nomes das colunas
    df_ecco_estoque_limpo.rename(
        columns={"codigo": "codigoProduto", "nome": "nomeProduto"},
        inplace=True,
    )

    # In[2]: Eccosys API: GET Listar todos os produtos

    url_prod = "https://empresa.eccosys.com.br/api/produtos?$offset=0&$count=1000000000&$dataConsiderada=data&$opcEcommerce=S"

    response_prod = requests.request(
        "GET", url_prod, headers=headers, data=payload
    )

    dic_ecco_prod = response_prod.json()
    df_ecco_prod = pd.DataFrame.from_dict(dic_ecco_prod)

    # COLUMNS TO KEEP
    columns_to_keep = [
        "id",
        "preco",
        "situacao",
    ]

    df_ecco_prod = df_ecco_prod[columns_to_keep]

    # Renomear os nomes das colunas
    df_ecco_prod.rename(
        columns={
            "id": "idProduto",
            "preco": "precoProduto",
            "situacao": "statusProduto",
        },
        inplace=True,
    )

    # Tranformar formato coluna idProduto para int
    columns_to_convert = ["idProduto"]
    df_ecco_prod[columns_to_convert] = df_ecco_prod[columns_to_convert].astype(
        int
    )

    # Trazer informação de produto para estoque
    df_ecco_estoque_final = df_ecco_estoque_limpo.merge(
        df_ecco_prod, on="idProduto", how="inner"
    )

    # In[11]: Enviar informações para DB

    from supabase import create_client, Client
    import supabase

    url: str = os.environ.get("SUPABASE_BI_URL")
    key: str = os.environ.get("SUPABASE_BI_KEY")
    supabase: Client = create_client(url, key)

    dic_ecco_estoque_final = df_ecco_estoque_final.to_dict(orient="records")

    try:
        response = (
            supabase.table(f"mage_bi_estoque_{client}_v1")
            .upsert(dic_ecco_estoque_final)
            .execute()
        )

    except Exception as exception:
        print(exception)
